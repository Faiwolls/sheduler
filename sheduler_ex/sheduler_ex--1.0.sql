-- Создаем схему
CREATE SCHEMA IF NOT EXISTS sheduler_ex;

-- Создаем таблицу задач
CREATE TABLE IF NOT EXISTS sheduler_ex.tasks (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    scheduled_time TIMESTAMPTZ NOT NULL,
    command TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

-- Создаем индекс для быстрого поиска задач
CREATE INDEX IF NOT EXISTS tasks_pending_idx
ON sheduler_ex.tasks (scheduled_time)
WHERE status = 'pending';

-- Функция добавления задачи
CREATE OR REPLACE FUNCTION sheduler_ex.add_task(
    task_name TEXT,
    task_time TIMESTAMPTZ,
    task_command TEXT
) RETURNS INTEGER AS $$
BEGIN
    -- Проверяем существование таблицы (на всякий случай)
    PERFORM 1 FROM information_schema.tables
    WHERE table_schema = 'sheduler_ex' AND table_name = 'tasks';

    -- Если таблицы нет - создаем
    IF NOT FOUND THEN
        CREATE TABLE sheduler_ex.tasks (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            scheduled_time TIMESTAMPTZ NOT NULL,
            command TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'running', 'completed', 'failed')),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ
        );

        -- Создаем индекс
        CREATE INDEX tasks_pending_idx
        ON sheduler_ex.tasks (scheduled_time)
        WHERE status = 'pending';
    END IF;

    -- Добавляем задачу
    INSERT INTO sheduler_ex.tasks (name, scheduled_time, command)
    VALUES (task_name, task_time, task_command)
    RETURNING id;
END;
$$ LANGUAGE plpgsql;

-- Функция запуска воркера
CREATE OR REPLACE FUNCTION sheduler_ex.launch_worker()
RETURNS VOID AS $$
DECLARE
    task_record RECORD;
BEGIN
    -- Проверяем существование таблицы
    PERFORM 1 FROM information_schema.tables
    WHERE table_schema = 'sheduler_ex' AND table_name = 'tasks';

    -- Если таблицы нет - выходим
    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Обрабатываем все готовые задачи
    FOR task_record IN
        SELECT * FROM sheduler_ex.tasks
        WHERE status = 'pending'
          AND scheduled_time <= NOW()
        ORDER BY scheduled_time
        FOR UPDATE SKIP LOCKED
    LOOP
        BEGIN
            -- Обновляем статус на "выполняется"
            UPDATE sheduler_ex.tasks
            SET status = 'running',
                started_at = NOW()
            WHERE id = task_record.id;

            -- Выполняем команду задачи
            EXECUTE task_record.command;

            -- Обновляем статус на "завершено"
            UPDATE sheduler_ex.tasks
            SET status = 'completed',
                completed_at = NOW()
            WHERE id = task_record.id;
        EXCEPTION WHEN OTHERS THEN
            -- В случае ошибки обновляем статус на "ошибка"
            UPDATE sheduler_ex.tasks
            SET status = 'failed',
                completed_at = NOW()
            WHERE id = task_record.id;

            -- Логируем ошибку
            RAISE WARNING 'Task % failed: %', task_record.id, SQLERRM;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Функция фонового воркера (бесконечный цикл)
CREATE OR REPLACE FUNCTION sheduler_ex.worker_loop()
RETURNS VOID AS $$
BEGIN
    LOOP
        -- Запускаем обработку задач
        PERFORM sheduler_ex.launch_worker();

        -- Ждем 5 секунд перед следующей проверкой
        PERFORM pg_sleep(5);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Автоматический запуск воркера при загрузке расширения
DO $$
BEGIN
    -- Проверяем, не запущен ли уже воркер
    IF NOT EXISTS (
        SELECT 1 FROM pg_stat_activity
        WHERE backend_type = 'background worker'
          AND application_name = 'sheduler_worker'
    ) THEN
        -- Запускаем воркер
        PERFORM pg_background_launch(
            'sheduler_ex.worker_loop',
            'sheduler_worker'
        );
    END IF;
EXCEPTION WHEN undefined_function THEN
    -- Если функция pg_background_launch не доступна
    RAISE NOTICE 'pg_background_launch not available, run worker manually';
END $$;
