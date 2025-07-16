\echo Use "CREATE EXTENSION sheduler_ex" to load this file. \quit
CREATE SCHEMA sheduler_ex;

-- Таблица для хранения задач
CREATE TABLE sheduler_ex.tasks (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    scheduled_time TIMESTAMPTZ NOT NULL,
    command TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

-- Индекс для быстрого поиска задач
CREATE INDEX tasks_pending_idx ON sheduler_ex.tasks (scheduled_time)
WHERE status = 'pending';

-- Функция для добавления задачи
CREATE OR REPLACE FUNCTION sheduler_ex.add_task(
    task_name TEXT,
    task_time TIMESTAMPTZ,
    task_command TEXT
) RETURNS INTEGER AS $$
INSERT INTO sheduler_ex.tasks (name, scheduled_time, command)
VALUES (task_name, task_time, task_command)
RETURNING id;
$$ LANGUAGE SQL VOLATILE;

-- Функция для запуска воркера
CREATE OR REPLACE FUNCTION sheduler_ex.launch_worker()
RETURNS VOID AS $$
DECLARE
    task_record RECORD;
BEGIN
    SELECT * INTO task_record
    FROM sheduler_ex.tasks
    WHERE status = 'pending'
      AND scheduled_time <= NOW()
    ORDER BY scheduled_time
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF FOUND THEN
        UPDATE sheduler_ex.tasks
        SET status = 'running',
            started_at = NOW()
        WHERE id = task_record.id;

        BEGIN
            EXECUTE task_record.command;
            UPDATE sheduler_ex.tasks
            SET status = 'completed',
                completed_at = NOW()
            WHERE id = task_record.id;
        EXCEPTION WHEN OTHERS THEN
            UPDATE sheduler_ex.tasks
            SET status = 'failed',
                completed_at = NOW()
            WHERE id = task_record.id;
            RAISE WARNING 'Task % failed: %', task_record.id, SQLERRM;
        END;
    END IF;
END;
$$ LANGUAGE plpgsql;
