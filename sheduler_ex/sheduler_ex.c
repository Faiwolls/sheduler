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

PG_MODULE_MAGIC;

/* Глобальные переменные для конфигурации */
static int sheduler_workers = 1;          // Количество воркеров по умолчанию
static int sheduler_poll_interval = 1000; // Интервал опроса в ms
static bool sheduler_enabled = true;      // Активация планировщика

static volatile sig_atomic_t got_sigterm = false;

void _PG_init(void);
void sheduler_main(Datum arg);
void handle_sigterm(SIGNAL_ARGS);

/* GUC-обработчики */
static void sheduler_workers_check_hook(int *newval, void **extra) {
    if (*newval < 1 || *newval > 128)
        *newval = 1;
}

static void sheduler_poll_interval_check_hook(int *newval, void **extra) {
    if (*newval < 100 || *newval > 60000)
        *newval = 1000;
}

void
_PG_init(void)
{
    // Регистрация параметров конфигурации
    DefineCustomIntVariable("sheduler_ex.workers",
                           "Number of worker processes",
                           NULL,
                           &sheduler_workers,
                           1,
                           1,
                           128,
                           PGC_SIGHUP,
                           GUC_CHECK_CONVERSION,
                           sheduler_workers_check_hook,
                           NULL,
                           NULL);
    
    DefineCustomIntVariable("sheduler_ex.poll_interval",
                           "Poll interval in milliseconds",
                           NULL,
                           &sheduler_poll_interval,
                           1000,
                           100,
                           60000,
                           PGC_SIGHUP,
                           GUC_UNIT_MS,
                           sheduler_poll_interval_check_hook,
                           NULL,
                           NULL);
    
    DefineCustomBoolVariable("sheduler_ex.enabled",
                            "Enable/disable the scheduler",
                            NULL,
                            &sheduler_enabled,
                            true,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);
    
    // Регистрация воркеров
    if (sheduler_enabled) {
        for (int i = 0; i < sheduler_workers; i++) {
            BackgroundWorker worker = {0};
            worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
            worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
            worker.bgw_restart_time = BGW_NEVER_RESTART;
            snprintf(worker.bgw_library_name, BGW_MAXLEN, "sheduler_ex");
            snprintf(worker.bgw_function_name, BGW_MAXLEN, "sheduler_main");
            snprintf(worker.bgw_name, BGW_MAXLEN, "Scheduler Worker #%d", i+1);
            worker.bgw_main_arg = Int32GetDatum(i);
            worker.bgw_notify_pid = 0;

            RegisterBackgroundWorker(&worker);
        }
    }
}

void
handle_sigterm(SIGNAL_ARGS)
{
    got_sigterm = true;
    SetLatch(MyLatch);
}

void
sheduler_main(Datum arg)
{
    int worker_id = DatumGetInt32(arg);
    int poll_interval = sheduler_poll_interval;
    MemoryContext scheduler_context = AllocSetContextCreate(TopMemoryContext,
                                                           "Scheduler Memory",
                                                           ALLOCSET_DEFAULT_SIZES);

    BackgroundWorkerUnblockSignals();
    pqsignal(SIGTERM, handle_sigterm);

    while (!got_sigterm)
    {
        int rc;
        MemoryContext old_context = MemoryContextSwitchTo(scheduler_context);

        /* Проверка активности планировщика */
        if (!sheduler_enabled) {
            ereport(LOG, (errmsg("Scheduler worker #%d exiting (disabled)", worker_id)));
            break;
        }

        /* Основной цикл обработки */
        WaitLatch(MyLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                  poll_interval,
                  WAIT_EVENT_BGWORKER_TIMEOUT);

        ResetLatch(MyLatch);

        if (got_sigterm)
            break;

        PG_TRY();
        {
            StartTransactionCommand();
            SPI_connect();
            
            // Выполнение задачи с обработкой в отдельном контексте
            SPI_execute("SELECT sheduler_ex.launch_worker()", false, 0);
            
            SPI_finish();
            CommitTransactionCommand();
        }
        PG_CATCH();
        {
            ErrorData *edata;
            MemoryContextSwitchTo(scheduler_context);
            edata = CopyErrorData();
            
            ereport(WARNING,
                   (errmsg("Scheduler worker #%d failed: %s", worker_id, edata->message),
                    errdetail("Task execution aborted")));
            
            FlushErrorState();
            FreeErrorData(edata);
            
            if (IsTransactionState())
                AbortCurrentTransaction();
        }
        PG_END_TRY();

        /* Очистка контекста памяти */
        MemoryContextReset(scheduler_context);
        MemoryContextSwitchTo(old_context);
    }

    /* Чистка при завершении */
    MemoryContextDelete(scheduler_context);
    proc_exit(0);
}
