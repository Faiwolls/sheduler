\echo Use "CREATE EXTENSION sheduler_ex" to load this file. \quit
CREATE SCHEMA sheduler_ex;

-- Таблица для хранения задач
CREATE TABLE sheduler_ex.tasks (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    scheduled_time TIMESTAMPTZ NOT NULL,
    command TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'canceled')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    attempts SMALLINT DEFAULT 0,
    max_attempts SMALLINT DEFAULT 3,
    worker_id INT
);

-- Индексы для оптимизации
CREATE INDEX tasks_pending_idx ON sheduler_ex.tasks (scheduled_time)
WHERE status = 'pending';

CREATE INDEX tasks_running_idx ON sheduler_ex.tasks (started_at)
WHERE status = 'running';

-- Конфигурация планировщика
CREATE TABLE sheduler_ex.config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT
);

INSERT INTO sheduler_ex.config (key, value, description) VALUES
('max_attempts', '3', 'Maximum task execution attempts'),
('retry_delay', '5 minutes', 'Delay between task retries'),
('lock_timeout', '10 seconds', 'Max task execution time');

-- Функция для добавления задачи
CREATE OR REPLACE FUNCTION sheduler_ex.add_task(
    task_name TEXT,
    task_time TIMESTAMPTZ,
    task_command TEXT,
    max_attempts INT DEFAULT NULL
) RETURNS BIGINT AS $$
INSERT INTO sheduler_ex.tasks (name, scheduled_time, command, max_attempts)
VALUES (
    task_name,
    task_time,
    task_command,
    COALESCE(max_attempts, (SELECT value::INT FROM sheduler_ex.config WHERE key = 'max_attempts'))
RETURNING id;
$$ LANGUAGE SQL VOLATILE;

-- Функция для запуска воркера
CREATE OR REPLACE FUNCTION sheduler_ex.launch_worker()
RETURNS VOID AS $$
DECLARE
    task_record RECORD;
    lock_timeout INTERVAL;
BEGIN
    -- Получаем настройки
    SELECT value::INTERVAL INTO lock_timeout 
    FROM sheduler_ex.config 
    WHERE key = 'lock_timeout';

    -- Блокируем задачу для обработки
    SELECT * INTO task_record
    FROM sheduler_ex.tasks
    WHERE status = 'pending'
      AND scheduled_time <= NOW()
      AND (attempts < max_attempts OR status != 'failed')
    ORDER BY scheduled_time
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Устанавливаем таймаут выполнения
    EXECUTE format('SET LOCAL lock_timeout = %L', lock_timeout);

    -- Обновляем статус задачи
    UPDATE sheduler_ex.tasks
    SET status = 'running',
        started_at = NOW(),
        attempts = attempts + 1,
        worker_id = pg_backend_pid()
    WHERE id = task_record.id
    RETURNING * INTO task_record;

    -- Выполняем команду
    BEGIN
        EXECUTE task_record.command;
        
        UPDATE sheduler_ex.tasks
        SET status = 'completed',
            completed_at = NOW()
        WHERE id = task_record.id;
    EXCEPTION WHEN OTHERS THEN
        -- Обработка ошибок с повторными попытками
        UPDATE sheduler_ex.tasks
        SET status = CASE 
                     WHEN attempts < max_attempts THEN 'pending' 
                     ELSE 'failed' 
                     END,
            scheduled_time = CASE 
                            WHEN attempts < max_attempts THEN 
                                NOW() + (SELECT value::INTERVAL 
                                         FROM sheduler_ex.config 
                                         WHERE key = 'retry_delay')
                            ELSE scheduled_time
                            END
        WHERE id = task_record.id;
        
        RAISE WARNING 'Task % failed: %', task_record.id, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция управления задачами
CREATE OR REPLACE FUNCTION sheduler_ex.cancel_task(task_id BIGINT)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE sheduler_ex.tasks
    SET status = 'canceled'
    WHERE id = task_id
    AND status IN ('pending', 'running');
    
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    RETURN true;
END;
$$ LANGUAGE plpgsql;

-- Мониторинг воркеров
CREATE VIEW sheduler_ex.worker_status AS
SELECT pid AS worker_pid,
       now() - state_change AS last_activity,
       state
FROM pg_stat_activity
WHERE backend_type = 'background worker'
AND application_name LIKE 'Scheduler Worker%';