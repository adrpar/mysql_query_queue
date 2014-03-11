/* Copyright (c) 2012, 2013, Adrian M. Partl, eScience Group at the
    Leibniz Institut for Astrophysics, Potsdam

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

/*****************************************************************
 ********              MYSQL_QUERY_QUEUE                   *******
 *****************************************************************
 *
 * mysql daemon that will provide a query queue to which you submit
 * jobs through UDFs. various queues can be defined, however no
 * execution optimisation is carried out yet.
 *
 *****************************************************************
 */

#define MYSQL_SERVER 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sql_priv.h"
#include <my_global.h>
#include <my_sys.h>
#include <mysql_version.h>
#include <sql_class.h>
#include <mysql/plugin.h>
#include <mysql.h>
#include <sql_parse.h>
#include "daemon_thd.h"
#include "sys_tbl.h"
#include "plugin_init.h"
#include "exec_query.h"
#include "query_queue.h"

#include <key.h>

#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50606
#include <global_threads.h>
#endif


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

long numQueriesParallel;
long intervalSec;
char recovery;
THD *thd;
#if MYSQL_VERSION_ID >= 50505
mysql_mutex_t qqueueKillMutex = PTHREAD_MUTEX_INITIALIZER;
mysql_cond_t qqueueKillCond = PTHREAD_COND_INITIALIZER;
#else
pthread_mutex_t qqueueKillMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t qqueueKillCond = PTHREAD_COND_INITIALIZER;
#endif

static pthread_t daemon_thread;

MYSQL_SYSVAR_LONG(numQueriesParallel, numQueriesParallel, NULL,
                  "Query queue number of parallel MySQL threads to execute", NULL, NULL, 2, 1, 10000000, 1);
MYSQL_SYSVAR_LONG(intervalSec, intervalSec, NULL,
                  "Query queue polling frequency of the head node", NULL, NULL, 5, 1, 10000000, 1);
MYSQL_SYSVAR_BOOL(recovery, recovery, NULL,
                  "Query queue job recovery after queue restart", NULL, NULL, true);

int queueRegisterThreadEnd(jobWorkerThd *job);
int queueRegisterThreadKill(jobWorkerThd *job);

struct st_mysql_sys_var *vars_system[] = {
    MYSQL_SYSVAR(numQueriesParallel),
    MYSQL_SYSVAR(intervalSec),
    MYSQL_SYSVAR(recovery),
    NULL
};

class activeQueueList {
public:
    int len;
    int numActive;
    jobWorkerThd **array;

#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_t numActiveMutex;
#ifdef HAVE_PSI_INTERFACE
    PSI_mutex_key key_numActiveMutex;
#endif
#else
    pthread_mutex_t numActiveMutex;
#endif

    activeQueueList() {
        array = NULL;
        len = 0;
        numActive = 0;

#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_init(key_numActiveMutex, &numActiveMutex, MY_MUTEX_INIT_FAST);
#else
        pthread_mutex_init(&numActiveMutex, MY_MUTEX_INIT_FAST);
#endif
    }

    ~activeQueueList() {
        if (array != NULL) {
            for (int i = 0; i < len; i++) {
                if (array[i] != NULL) {
                    if (array[i]->job != NULL) {
                        delete array[i]->job;
                    }
                    delete array[i];
                }
            }

            my_free(array);
        }
    }

    int resize(long newLen) {
        if (newLen > len) {
            //make bigger
            if (array != NULL) {
                array = (jobWorkerThd **) my_realloc(array, newLen * sizeof (jobWorkerThd *), MYF(0));

                for (int i = 0; i < (newLen - len); i++) {
                    array[len + i] = NULL;
                }
            } else {
                array = (jobWorkerThd **) my_malloc(newLen * sizeof (jobWorkerThd *), MYF(0));
                memset(array, 0, newLen * sizeof (jobWorkerThd *));
            }

            //check if successful
            if (array == NULL)
                return 1;

            len = newLen;
        }

        return 0;
    }

    int registerJob(qqueue_jobs_row *thisJob) {
#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_lock(&numActiveMutex);
#else
        pthread_mutex_lock(&numActiveMutex);
#endif

        jobWorkerThd *job = new jobWorkerThd();
        job->job = thisJob;
        job->thd = new THD;
        job->thdTerm = queueRegisterThreadEnd;
        job->thdKillHandler = queueRegisterThreadKill;
        job->killReasonTimeout = false;

        if (len - numActive <= 0)
            return 1;

        //look for a free spot in the array
        for (int i = 0; i < len; i++) {
            if (array[i] == NULL) {
                array[i] = job;
                break;
            }
        }

        //register start of execution
        registerThreadStart(job);

        init_worker_thread(job);

        numActive++;

#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_unlock(&numActiveMutex);
#else
        pthread_mutex_unlock(&numActiveMutex);
#endif

        return 0;
    }

    int unregisterJob(jobWorkerThd *thisJob) {
#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_lock(&numActiveMutex);
#else
        pthread_mutex_lock(&numActiveMutex);
#endif

        //look for this job in the array and unregister
        for (int i = 0; i < len; i++) {
            if (array[i] == thisJob) {
                array[i] = NULL;
                numActive--;
                break;
            }
        }

#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_unlock(&numActiveMutex);
#else
        pthread_mutex_unlock(&numActiveMutex);
#endif

        return 0;
    }

    int unregisterAndStartNewJob(jobWorkerThd *thisJob) {
        //look for this job in the array and unregister
        for (int i = 0; i < len; i++) {
            if (array[i] == thisJob) {

                int error = 0;
                Open_tables_backup backup;
                TABLE *tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &backup, false, &error);
                if (error || (tbl == NULL && error != HA_STATUS_NO_LOCK) ) {
                    fprintf(stderr, "registerThreadEnd: error in opening jobs sys table: error: %i\n", error);
                    close_sysTbl(current_thd, tbl, &backup);
                    return 1;
                }

                qqueue_jobs_row **jobArray = getHighestPriorityJob(tbl, 1);
                close_sysTbl(current_thd, tbl, &backup);

                if (jobArray == NULL) {
#if MYSQL_VERSION_ID >= 50505
                    mysql_mutex_lock(&numActiveMutex);
#else
                    pthread_mutex_lock(&numActiveMutex);
#endif

                    array[i] = NULL;
                    numActive--;

#if MYSQL_VERSION_ID >= 50505
                    mysql_mutex_unlock(&numActiveMutex);
#else
                    pthread_mutex_unlock(&numActiveMutex);
#endif

                } else {
                    if (jobArray[0] != NULL) {
                        jobWorkerThd *job = new jobWorkerThd();
                        job->job = jobArray[0];
                        job->thdTerm = queueRegisterThreadEnd;
                        job->thdKillHandler = queueRegisterThreadKill;
                        job->killReasonTimeout = false;

                        array[i] = job;

                        //register start of execution
                        registerThreadStart(job);

                        init_worker_thread(job);
                    } else {
#if MYSQL_VERSION_ID >= 50505
                        mysql_mutex_lock(&numActiveMutex);
#else
                        pthread_mutex_lock(&numActiveMutex);
#endif

                        array[i] = NULL;
                        numActive--;

#if MYSQL_VERSION_ID >= 50505
                        mysql_mutex_unlock(&numActiveMutex);
#else
                        pthread_mutex_unlock(&numActiveMutex);
#endif
                    }
                }

                if(jobArray != NULL)
                    my_free(jobArray);
                break;
            }
        }

        return 0;
    }

    int killJob(ulong id) {
        //look for this job in the array and kill
        bool found = 0;

        for (int i = 0; i < len; i++) {
            if (array[i] != NULL) {
                if (array[i]->job->id == id) {
                    //kill job
                    registerThreadEnd(array[i], true, false);
                    sql_kill(array[i]->thd, array[i]->thd->thread_id, 0);
                    found = 1;
                    break;
                }
            }
        }

        return found;
    }

    int timeoutJob(jobWorkerThd *thisJob) {
        //kill job
        thisJob->killReasonTimeout = true;
        registerThreadEnd(thisJob, false, true);
        sql_kill(thisJob->thd, thisJob->thd->thread_id, 0);

        return 0;
    }

};

activeQueueList queueList;

pthread_handler_t qqueue_daemon(void *p) {
    THD *thd = (THD *) p;
    bool res;
    int tmp;

    my_thread_init();
    res = post_init_daemon_thread(thd);

    TABLE *tbl;
    Open_tables_backup backup;
    int error = 0;

    char time_str[20];

    get_date(time_str, GETDATE_DATE_TIME, 0);
    fprintf(stderr, "Query queue daemon thread started at %s\n", time_str);

    //we need to clean up the queue first. it could be, that the server stopped and
    //some jobs were left hanging in the wild. we offer two options: either set them
    //all to an error state or set them to pending again (which would fail if tables
    //need to be created an they are already there... this is NOT handeled yet.)
    int numChanges = 0;
    if (recovery == true) {
        numChanges = resetJobQueue(QUEUE_PENDING);
    } else {
        numChanges = resetJobQueue(QUEUE_ERROR);
    }

    thd->proc_info = "Daemon running";

    while (thd->killed == 0) {
        if (queueList.resize(numQueriesParallel)) {
            fprintf(stderr, "Query queue daemon: error allocating memory. Need to stop now...\n");
            break;
        }

        int error = 0;
        tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &backup, false, &error);
        if (error || (tbl == NULL && error != HA_STATUS_NO_LOCK) ) {
            fprintf(stderr, "qqueue_daemon: error in opening jobs sys table: error: %i\n", error);
            close_sysTbl(current_thd, tbl, &backup);
            break;
        }

#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_lock(&queueList.numActiveMutex);
#else
        pthread_mutex_lock(&queueList.numActiveMutex);
#endif

        int numActiveJobs = queueList.numActive;

#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_unlock(&queueList.numActiveMutex);
#else
        pthread_mutex_unlock(&queueList.numActiveMutex);
#endif

        int numEmptySlots = queueList.len - numActiveJobs;

        qqueue_jobs_row **jobArray = getHighestPriorityJob(tbl, numEmptySlots);

#ifdef __QQUEUE_DEBUG__
        fprintf(stderr, "Empty slots: %i\n", numEmptySlots);
        if(jobArray != NULL) {
            for (int i = 0; i < numEmptySlots + 1; i++) {
                if (jobArray[i] == NULL)
                    break;

                fprintf(stderr, "Job: %i, Status: %i, Priority: %i Query: %s\n", jobArray[i]->id, jobArray[i]->status,
                        jobArray[i]->priority, jobArray[i]->query);
            }
        }
#endif

        close_sysTbl(current_thd, tbl, &backup);

        //register new jobs with execution queue
        for (int i = 0; i < numEmptySlots + 1; i++) {
            if (jobArray == NULL)
                break;

            if (jobArray[i] == NULL)
                break;

            queueList.registerJob(jobArray[i]);
        }

        if(jobArray != NULL)
            my_free(jobArray);

        //check if running queries have reached their timeout
#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_lock(&queueList.numActiveMutex);
#else
        pthread_mutex_lock(&queueList.numActiveMutex);
#endif
        time_t now = my_time(0);
        for (int i = 0; i < queueList.len; i++) {
            if (queueList.array[i] == NULL)
                continue;

            if (queueList.array[i]->thd == NULL)
                continue;

            THD *currThd = queueList.array[i]->thd;

#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50605
            if (currThd->start_time.tv_sec) {
                int runtime = (longlong) (now - currThd->start_time.tv_sec);
#else
            if (currThd->start_time) {
                int runtime = (longlong) (now - currThd->start_time);
#endif
                qqueue_queues_row *queue = getQueueByID(queueList.array[i]->job->queue);

                if (runtime >= queue->timeout) {
                    //kill query
                    queueList.timeoutJob(queueList.array[i]);
                }
            }
        }

#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_unlock(&queueList.numActiveMutex);
#else
        pthread_mutex_unlock(&queueList.numActiveMutex);
#endif

        //get the time for sleep
        for (int i = 0; i < intervalSec; i++) {
            if (thd->killed != 0) {
                break;
            }

            struct timespec deltaTime = {0, 0};
            deltaTime.tv_sec = time(NULL) + 1;
#if MYSQL_VERSION_ID >= 50505
            mysql_mutex_lock(&qqueueKillMutex);
            tmp = mysql_cond_timedwait(&qqueueKillCond, &qqueueKillMutex, &deltaTime);
            mysql_mutex_unlock(&qqueueKillMutex);
#else
            pthread_mutex_lock(&qqueueKillMutex);
            tmp = pthread_cond_timedwait(&qqueueKillCond, &qqueueKillMutex, &deltaTime);
            pthread_mutex_unlock(&qqueueKillMutex);
#endif

            if (tmp != ETIMEDOUT) {
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50500
                thd->killed = KILL_CONNECTION;
#else
                thd->killed = THD::KILL_CONNECTION;
#endif
                break;
            }
        }
    }

    get_date(time_str, GETDATE_DATE_TIME, 0);
    fprintf(stderr, "Query queue daemon thread ended at %s\n", time_str);

    deinit_thread(&thd);
    my_thread_end();
    pthread_exit(0);

    return NULL;
}

static int qqueue_plugin_init(void *p) {
    pthread_attr_t attr;
    char daemon_filename[FN_REFLEN];
    char buffer[1024];
    char time_str[20];

    get_date(time_str, GETDATE_DATE_TIME, 0);
    fprintf(stderr, "Query queue daemon started at %s\n", time_str);

    THD *new_thd = NULL;

    if (!(new_thd = new THD)) {
        fprintf(stderr, "Query queue - query_queue ERROR: Could not create thread!\n");
        return 1;
    }

    pre_init_daemon_thread(new_thd);
    new_thd->system_thread = SYSTEM_THREAD_EVENT_SCHEDULER;

#if MYSQL_VERSION_ID < 50600
    new_thd->command = COM_DAEMON;
#else
    new_thd->set_command(COM_DAEMON);
#endif

    new_thd->security_ctx->master_access |= SUPER_ACL;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&daemon_thread, &attr, qqueue_daemon, new_thd) != 0) {
        new_thd->proc_info = "Clearing";
        net_end(&new_thd->net);
        mysql_mutex_lock(&LOCK_thread_count);

#if defined(MARIADB_BASE_VERSION) || MYSQL_VERSION_ID < 50606
        --thread_count;
#else
        remove_global_thread(new_thd);
#endif

        delete new_thd;
        mysql_cond_broadcast(&COND_thread_count);
        mysql_mutex_unlock(&LOCK_thread_count);
        fprintf(stderr, "Query queue - query_queue ERROR: Could not create thread!\n");
        return 1;
    }

    if (mysqld_server_started) {
        //read user groups table
        int error = 0;
        Open_tables_backup backup;
        TABLE *tbl = open_sysTbl(current_thd, "qqueue_usrGrps", strlen("qqueue_usrGrps"), &backup, false, &error);
        if (error) {
            fprintf(stderr, "qqueue_plugin_init: error in opening usrGrps sys table: error %i\n", error);
        } else {
            loadQqueueUsrGrps(tbl);
        }
        close_sysTbl(current_thd, tbl, &backup);

        tbl = open_sysTbl(current_thd, "qqueue_queues", strlen("qqueue_queues"), &backup, false, &error);
        if (error) {
            fprintf(stderr, "qqueue_plugin_init: error in opening usrGrps sys table: error %i\n", error);
        } else {
            loadQqueueQueues(tbl);
        }
        close_sysTbl(current_thd, tbl, &backup);
    }

    setPluginInstalled();

    return 0;
}

static int qqueue_plugin_deinit(void *p) {
    char buffer[1024];
    char time_str[20];

    if (thd != NULL) {
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50500
        thd->killed = KILL_CONNECTION;
#else
        thd->killed = THD::KILL_CONNECTION;
#endif
    }
#if MYSQL_VERSION_ID >= 50505
    mysql_cond_signal(&qqueueKillCond);
#else
    pthread_cond_signal(&qqueueKillCond);
#endif
    pthread_join(daemon_thread, NULL);

    get_date(time_str, GETDATE_DATE_TIME, 0);
    fprintf(stderr, "Query queue daemon stopped at %s\n", time_str);

    unsetPluginInstalled();

    return 0;
}

int queueRegisterThreadEnd(jobWorkerThd *job) {
    registerThreadEnd(job, false, false);

    queueList.unregisterAndStartNewJob(job);

    return 0;
}

int queueRegisterThreadKill(jobWorkerThd *job) {
    //fooling mysql to properly register things...
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50500
    killed_state oldKill = job->thd->killed;
    job->thd->killed = NOT_KILLED;
#else
    THD::killed_state oldKill = job->thd->killed;
    job->thd->killed = THD::NOT_KILLED;
#endif

    queueList.unregisterAndStartNewJob(job);

    job->thd->killed = oldKill;

    return 0;
}

int registerJobKill(ulong id) {
    queueList.killJob(id);

    return 0;
}

void lockQueue() {
#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_lock(&queueList.numActiveMutex);
#else
    pthread_mutex_lock(&queueList.numActiveMutex);
#endif
}

void unlockQueue() {
#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_unlock(&queueList.numActiveMutex);
#else
    pthread_mutex_unlock(&queueList.numActiveMutex);
#endif
}

struct st_mysql_daemon vars_plugin_info = {MYSQL_DAEMON_INTERFACE_VERSION};

mysql_declare_plugin(vars) {
    MYSQL_DAEMON_PLUGIN,
    &vars_plugin_info,
    "qqueue",
    "Adrian M. Partl",
    "Query queue for MySQL query jobs",
    PLUGIN_LICENSE_GPL,
    qqueue_plugin_init,
    qqueue_plugin_deinit,
    0x0100,
    NULL,
    vars_system,
    NULL
}
mysql_declare_plugin_end;
