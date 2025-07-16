#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "libpq-fe.h"

PG_MODULE_MAGIC;

static BackgroundWorker worker;
static volatile sig_atomic_t got_sigterm = false;

void _PG_init(void);
void sheduler_main(Datum arg);

void handle_sigterm(SIGNAL_ARGS);

void
_PG_init(void)
{
    BackgroundWorker worker = {0};
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "sheduler_ex");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "sheduler_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "Sheduler Worker");
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
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
    BackgroundWorkerUnblockSignals();
    pqsignal(SIGTERM, handle_sigterm);
    while (!got_sigterm)
    {
        int rc;
        WaitLatch(MyLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                  5000L,
                  WAIT_EVENT_BGWORKER_TIMEOUT);

        ResetLatch(MyLatch);

        if (got_sigterm)
            break;

        StartTransactionCommand();
        SPI_connect();
        SPI_execute("SELECT sheduler_ex.launch_worker()", false, 0);
        SPI_finish();
        CommitTransactionCommand();
    }

    proc_exit(0);
}
