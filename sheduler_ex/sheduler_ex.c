#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "libpq-fe.h"
#include "utils/guc.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "pgstat.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/* GUC переменные */
static int scheduler_interval = 5000;
static int max_workers = 5;
static volatile sig_atomic_t got_sigterm = false;

void _PG_init(void);
void scheduler_main(Datum arg);
void worker_main(Datum arg);
void handle_sigterm(SIGNAL_ARGS);

/* GUC обработчики */
static bool scheduler_interval_check(int *newval, void **extra, GucSource source) {
    if (*newval < 100 || *newval > 60000) *newval = 5000;
    return true;
}

static bool max_workers_check(int *newval, void **extra, GucSource source) {
    if (*newval < 1 || *newval > 100) *newval = 5;
    return true;
}

void _PG_init(void) {
    BackgroundWorker scheduler;
    
    /* Регистрация параметров конфигурации */
    DefineCustomIntVariable("sheduler_ex.scheduler_interval",
                           "Main scheduler loop interval in milliseconds",
                           NULL, &scheduler_interval, 5000,
                           100, 60000, PGC_SIGHUP,
                           GUC_UNIT_MS, scheduler_interval_check,
                           NULL, NULL);

    DefineCustomIntVariable("sheduler_ex.max_workers",
                           "Maximum number of concurrent workers",
                           NULL, &max_workers, 5,
                           1, 100, PGC_SIGHUP,
                           0, max_workers_check, NULL, NULL);

    /* Регистрация главного планировщика */
    memset(&scheduler, 0, sizeof(BackgroundWorker));
    scheduler.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    scheduler.bgw_start_time = BgWorkerStart_RecoveryFinished;
    scheduler.bgw_restart_time = BGW_NEVER_RESTART;
    snprintf(scheduler.bgw_library_name, BGW_MAXLEN, "sheduler_ex");
    snprintf(scheduler.bgw_function_name, BGW_MAXLEN, "scheduler_main");
    snprintf(scheduler.bgw_name, BGW_MAXLEN, "Task Scheduler");
    scheduler.bgw_main_arg = (Datum) 0;
    scheduler.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&scheduler);
}

void handle_sigterm(SIGNAL_ARGS) {
    got_sigterm = true;
    SetLatch(MyLatch);
}

/* Главный процесс-планировщик */
void scheduler_main(Datum arg) {
    int ret;
    int running_tasks = 0;
    int available_slots;
    const char *count_query;
    const char *task_query;
    char formatted_query[512];
    uint64 i;
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    BgwHandleStatus status;
    pid_t worker_pid;
    
    BackgroundWorkerUnblockSignals();
    pqsignal(SIGTERM, handle_sigterm);
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    while (!got_sigterm) {
        WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                  scheduler_interval, 0);
        ResetLatch(MyLatch);

        if (got_sigterm) break;

        StartTransactionCommand();
        if (SPI_connect() != SPI_OK_CONNECT) {
            elog(WARNING, "SPI_connect failed in scheduler");
            CommitTransactionCommand();
            continue;
        }

        /* Получаем активные задачи */
        count_query = "SELECT count(*) FROM sheduler_ex.tasks "
                     "WHERE status = 'running'";
        ret = SPI_execute(count_query, true, 0);
        running_tasks = 0;
        
        if (ret == SPI_OK_SELECT && SPI_processed > 0) {
            bool isnull = false;
            running_tasks = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], 
                                           SPI_tuptable->tupdesc, 1, &isnull));
        }

        /* Рассчитываем доступные слоты */
        available_slots = max_workers - running_tasks;
        if (available_slots <= 0) {
            SPI_finish();
            CommitTransactionCommand();
            continue;
        }

        /* Получаем задачи для выполнения */
        task_query = "SELECT id, command, task_type "
                    "FROM sheduler_ex.tasks "
                    "WHERE status = 'pending' AND scheduled_time <= NOW() "
                    "ORDER BY priority DESC, scheduled_time "
                    "LIMIT %d "
                    "FOR UPDATE SKIP LOCKED";
        snprintf(formatted_query, sizeof(formatted_query), task_query, available_slots);
        
        ret = SPI_execute(formatted_query, true, 0);
        if (ret != SPI_OK_SELECT) {
            elog(WARNING, "SPI_execute failed: %d", ret);
            SPI_finish();
            CommitTransactionCommand();
            continue;
        }

        /* Запускаем воркеры для задач */
        for (i = 0; i < SPI_processed; i++) {
            bool isnull;
            HeapTuple tuple = SPI_tuptable->vals[i];
            int task_id;
            
            /* Получаем ID задачи */
            task_id = DatumGetInt32(SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull));
            if (isnull) {
                elog(WARNING, "Task id is NULL");
                continue;
            }
            
            /* Запускаем воркер */
            memset(&worker, 0, sizeof(BackgroundWorker));
            worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
            worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
            worker.bgw_restart_time = BGW_NEVER_RESTART;
            snprintf(worker.bgw_library_name, BGW_MAXLEN, "sheduler_ex");
            snprintf(worker.bgw_function_name, BGW_MAXLEN, "worker_main");
            snprintf(worker.bgw_name, BGW_MAXLEN, "Task Worker #%d", task_id);
            worker.bgw_main_arg = Int32GetDatum(task_id);
            worker.bgw_notify_pid = MyProcPid;

            if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
                elog(WARNING, "Failed to register worker for task %d", task_id);
                continue;
            }

            /* Ждем запуска воркера */
            status = WaitForBackgroundWorkerStartup(handle, &worker_pid);
            if (status != BGWH_STARTED) {
                elog(WARNING, "Worker for task %d failed to start", task_id);
                continue;
            }

            /* Обновляем статус задачи */
            const char *update_query = "UPDATE sheduler_ex.tasks "
                                       "SET status = 'running', started_at = NOW(), worker_pid = $1 "
                                       "WHERE id = $2";
            ret = SPI_execute_params(update_query,
                                     2,
                                     (Oid[]) { INT4OID, INT4OID },
                                     (Datum[]) { Int32GetDatum(worker_pid), Int32GetDatum(task_id) },
                                     NULL,
                                     false,
                                     0);
            if (ret != SPI_OK_UPDATE) {
                elog(WARNING, "Failed to update task %d to running: %d", task_id, ret);
            }
        }

        SPI_finish();
        CommitTransactionCommand();
    }

    proc_exit(0);
}

/* Процесс-воркер */
void worker_main(Datum arg) {
    int task_id = DatumGetInt32(arg);
    MemoryContext worker_context;
    MemoryContext old_context;
    char *command = NULL;
    char *task_type = NULL;
    char *command_copy = NULL;
    char *task_type_copy = NULL;
    bool success = false;
    char query[256];
    int ret;
    
    worker_context = AllocSetContextCreate(TopMemoryContext,
                                          "Worker Context",
                                          ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(worker_context);
    
    BackgroundWorkerUnblockSignals();
    pqsignal(SIGTERM, handle_sigterm);
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    /* Получаем данные задачи */
    StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) {
        elog(WARNING, "SPI_connect failed in worker for task %d", task_id);
        CommitTransactionCommand();
        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(worker_context);
        proc_exit(1);
    }
    
    snprintf(query, sizeof(query),
             "SELECT command, task_type "
             "FROM sheduler_ex.tasks "
             "WHERE id = %d", task_id);
    
    ret = SPI_execute(query, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0) {
        HeapTuple tuple = SPI_tuptable->vals[0];
        /* Копируем данные в контекст воркера */
        command_copy = pstrdup(SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1));
        task_type_copy = pstrdup(SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2));
    } else {
        elog(WARNING, "Task %d not found", task_id);
        SPI_finish();
        CommitTransactionCommand();
        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(worker_context);
        proc_exit(1);
    }
    
    SPI_finish();
    CommitTransactionCommand();
    
    /* Проверка данных */
    if (command_copy == NULL || task_type_copy == NULL) {
        elog(WARNING, "Task %d data incomplete", task_id);
        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(worker_context);
        proc_exit(1);
    }

    /* Выполняем задачу */
    PG_TRY();
    {
        if (strcmp(task_type_copy, "SQL") == 0) {
            /* Для SQL задач выполняем в транзакции */
            StartTransactionCommand();
            SPI_connect();
            ret = SPI_execute(command_copy, false, 0);
            if (ret < 0) {
                elog(WARNING, "SQL task failed: %s", SPI_result_code_string(ret));
            } else {
                success = true;
            }
            SPI_finish();
            CommitTransactionCommand();
        } else if (strcmp(task_type_copy, "SHELL") == 0) {
            /* Для SHELL выполняем вне транзакции */
            int status = system(command_copy);
            if (status != 0) {
                elog(WARNING, "Shell command failed with status %d", status);
            } else {
                success = true;
            }
        } else {
            elog(ERROR, "Unknown task type: %s", task_type_copy);
        }
    }
    PG_CATCH();
    {
        ErrorData *edata = CopyErrorData();
        elog(WARNING, "Task execution failed: %s", edata->message);
        FlushErrorState();
        FreeErrorData(edata);
        success = false;
    }
    PG_END_TRY();

    /* Обновляем статус задачи */
    StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) {
        elog(WARNING, "SPI_connect failed in worker for updating task %d", task_id);
        CommitTransactionCommand();
        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(worker_context);
        proc_exit(1);
    }

    const char *update_query = "UPDATE sheduler_ex.tasks "
                               "SET status = $1, completed_at = NOW() "
                               "WHERE id = $2";
    const char *status_str = success ? "completed" : "failed";
    ret = SPI_execute_params(update_query,
                             2,
                             (Oid[]) { TEXTOID, INT4OID },
                             (Datum[]) { CStringGetTextDatum(status_str), Int32GetDatum(task_id) },
                             NULL,
                             false,
                             0);
    if (ret != SPI_OK_UPDATE) {
        elog(WARNING, "Failed to update task %d status to %s: %d", task_id, status_str, ret);
    }

    SPI_finish();
    CommitTransactionCommand();

    /* Очистка */
    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(worker_context);
    proc_exit(0);
}