\echo Use "CREATE EXTENSION sheduler_ex" to load this file. \quit

-- Создаем схему только если она не существует
CREATE SCHEMA IF NOT EXISTS sheduler_ex;
COMMENT ON SCHEMA sheduler_ex IS 'Scheduler extension schema';

-- Таблица для хранения задач
CREATE TABLE sheduler_ex.tasks (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    scheduled_time TIMESTAMPTZ NOT NULL,
    command TEXT NOT NULL,
    task_type TEXT NOT NULL DEFAULT 'SQL' CHECK (task_type IN ('SQL', 'SHELL')),
    status TEXT NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    priority INTEGER NOT NULL DEFAULT 5,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    worker_pid INTEGER
);

-- Индексы для оптимизации
CREATE INDEX tasks_pending_idx ON sheduler_ex.tasks (scheduled_time, priority)
WHERE status = 'pending';

CREATE INDEX tasks_running_idx ON sheduler_ex.tasks (started_at)
WHERE status = 'running';

-- Функция для добавления задачи
CREATE FUNCTION sheduler_ex.add_task(
    task_name TEXT,
    task_time TIMESTAMPTZ,
    task_command TEXT,
    task_type TEXT DEFAULT 'SQL',
    task_priority INTEGER DEFAULT 5
) RETURNS BIGINT AS $$
INSERT INTO sheduler_ex.tasks (name, scheduled_time, command, task_type, priority)
VALUES (task_name, task_time, task_command, task_type, task_priority)
RETURNING id;
$$ LANGUAGE SQL VOLATILE;

-- Функция отмены задачи
CREATE FUNCTION sheduler_ex.cancel_task(task_id BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
    worker_pid INT;
BEGIN
    SELECT worker_pid INTO worker_pid 
    FROM sheduler_ex.tasks 
    WHERE id = task_id 
    AND status = 'running';

    IF FOUND THEN
        PERFORM pg_cancel_backend(worker_pid);
    END IF;

    UPDATE sheduler_ex.tasks
    SET status = 'failed'
    WHERE id = task_id
    AND status IN ('pending', 'running');
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- Мониторинг задач
CREATE VIEW sheduler_ex.active_tasks AS
SELECT 
    id, 
    name, 
    status, 
    EXTRACT(EPOCH FROM (NOW() - started_at)) AS duration_sec
FROM sheduler_ex.tasks
WHERE status IN ('running', 'pending');

-- Мониторинг воркеров
CREATE VIEW sheduler_ex.worker_status AS
SELECT 
    pid, 
    application_name, 
    state,
    now() - state_change AS state_duration
FROM pg_stat_activity
WHERE backend_type = 'background worker'
AND application_name LIKE 'Task Worker%';