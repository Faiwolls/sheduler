\set ON_ERROR_STOP on

-- 1. Подготовка среды
DROP EXTENSION IF EXISTS sheduler_ex CASCADE;
CREATE EXTENSION sheduler_ex;

-- 2. Тест базовых объектов
SELECT 'Tables' as test, COUNT(*) = 1 AS status
FROM pg_tables 
WHERE schemaname = 'sheduler_ex' AND tablename = 'tasks';

SELECT 'Functions' as test, COUNT(*) >= 3 AS status
FROM pg_proc 
WHERE pronamespace = 'sheduler_ex'::regnamespace;

SELECT 'Views' as test, COUNT(*) = 2 AS status
FROM pg_views 
WHERE schemaname = 'sheduler_ex';

-- 3. Тест добавления задач
SELECT 'Add SQL task' as test, 
       sheduler_ex.add_task(
           'test_sql',
           NOW(),
           'CREATE TEMP TABLE IF NOT EXISTS test_table (id SERIAL)',
           'SQL'
       ) > 0 AS status;

SELECT 'Add SHELL task' as test, 
       sheduler_ex.add_task(
           'test_shell',
           NOW(),
           'touch /tmp/sheduler_test_file',
           'SHELL'
       ) > 0 AS status;

-- 4. Проверка добавленных задач
SELECT 'Check pending tasks' as test, 
       COUNT(*) = 2 AS status
FROM sheduler_ex.tasks 
WHERE status = 'pending';

-- 5. Имитация работы планировщика (вручную запускаем worker)
DO $$
DECLARE 
    task_id INT;
    task_record RECORD;
BEGIN
    -- Находим задачу для выполнения
    SELECT id, command, task_type INTO task_record
    FROM sheduler_ex.tasks
    WHERE status = 'pending'
    LIMIT 1;
    
    IF FOUND THEN
        -- Обновляем статус как это сделал бы планировщик
        UPDATE sheduler_ex.tasks
        SET status = 'running', started_at = NOW()
        WHERE id = task_record.id;
        
        -- Выполняем задачу как worker
        PERFORM sheduler_ex.worker_main(task_record.id::integer);
    END IF;
END $$;

-- 6. Проверка результатов выполнения
SELECT 'SQL task result' as test, 
       EXISTS(
           SELECT 1 FROM pg_tables 
           WHERE tablename = 'test_table'
       ) AS status;

SELECT 'SHELL task result' as test, 
       pg_ls_dir('/tmp') @> ARRAY['sheduler_test_file'] AS status;

SELECT 'Task statuses' as test, 
       COUNT(*) = 2 AS status
FROM sheduler_ex.tasks 
WHERE status IN ('completed', 'failed');

-- 7. Тест отмены задачи
SELECT 'Cancel task' as test, 
       sheduler_ex.cancel_task(
           (SELECT id FROM sheduler_ex.tasks ORDER BY id DESC LIMIT 1)
       ) AS status;

-- 8. Проверка представлений
SELECT 'Active tasks view' as test, 
       COUNT(*) >= 0 AS status
FROM sheduler_ex.active_tasks;

SELECT 'Worker status view' as test, 
       COUNT(*) >= 0 AS status
FROM sheduler_ex.worker_status;

-- 9. Очистка
DROP EXTENSION sheduler_ex CASCADE;
\! rm -f /tmp/sheduler_test_file
