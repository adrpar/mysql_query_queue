/*  Copyright (c) 2012, 2013, Adrian M. Partl, eScience Group at the
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
 ********                   exec_query                     *******
 *****************************************************************
 *
 * functions for creating a new thread that will execute a job/query
 * this code is highly inspired by sql/sql_parse.cc
 *
 *****************************************************************
 */

#define MYSQL_SERVER 1

#include <stdio.h>
#include <stdlib.h>
#include <cmath>
#include <sql_class.h>
#include <sql_parse.h>
#include <sql_connect.h>
#include <sql_audit.h>
#include <sql_acl.h>
#include <tztime.h>
#include <probes_mysql_nodtrace.h>
#include "exec_query.h"
#include "daemon_thd.h"
#include "sql_query.h"

#ifdef WITH_PERFSCHEMA_STORAGE_ENGINE
#include <storage/perfschema/pfs_server.h>
#endif /* WITH_PERFSCHEMA_STORAGE_ENGINE */


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

pthread_handler_t worker_thread(void *arg) {
    jobWorkerThd *jobArg = (jobWorkerThd *) arg;

    init_thread(&(jobArg->thd), "stating thread...", false);

    Security_context *old;
    Security_context newContext;
    LEX_STRING user;
    LEX_STRING host;
    LEX_STRING db;

    char *usrStr = my_strdup("root", MYF(0));
    char *hostStr = my_strdup("localhost", MYF(0));

    user.str = usrStr;
    user.length = strlen(usrStr);
    host.str = hostStr;
    host.length = strlen(hostStr);
    db.str = "mysql";
    db.length = strlen("mysql");

#ifdef HAVE_PSI_THREAD_INTERFACE
      /*
        Create new instrumentation for the new THD job,
        and attach it to this running pthread.
      */
      PSI_thread *psi= PSI_THREAD_CALL(new_thread)(key_thread_one_connection,
                                                   jobArg->thd, jobArg->thd->thread_id);
      PSI_THREAD_CALL(set_thread)(psi);
#endif

    newContext.change_security_context(jobArg->thd, &user, &host, &db, &old);

    int err;
    if (jobArg->thd->killed == 0) {
        err = workload(jobArg);
    }

    jobArg->thd->security_ctx->restore_security_context(jobArg->thd, old);

#ifdef HAVE_PSI_THREAD_INTERFACE
    /*
      Delete the instrumentation for the job that just completed,
      before parking this pthread in the cache (blocked on COND_thread_cache).
    */
    PSI_THREAD_CALL(delete_current_thread)();
#endif

    //callback function to handle management of thread termination and processing
    //of new thread
    if (jobArg->thdTerm != NULL && jobArg->thd->killed == 0) 
        (*jobArg->thdTerm)(jobArg);

#ifndef __QQUEUE_NOWAIT_ON_KILL_TO_JOBRESTART__
    if (jobArg->thd->killed != 0)
        (*jobArg->thdKillHandler)(jobArg);
#endif

    my_free(usrStr);
    my_free(hostStr);

    deinit_thread(&(jobArg->thd));

    if (jobArg->error != NULL)
        my_free(jobArg->error);

    delete jobArg->job;
    delete jobArg;

    my_thread_end();
    pthread_exit(0);
    return NULL;
}

int workload(jobWorkerThd *jobArg) {
    char *jobDes;
    char *queryCpy;

    jobArg->error = NULL;

    jobDes = (char *) my_malloc(strlen(jobArg->job->actualQuery) +
                                strlen("JobWorker: U:  P:  Q:  ") +
                                (int) log10((jobArg->job->usrId > 0 ? jobArg->job->usrId : 1)) + 2 + 10, MYF(0)); //2 = \0 and log10 roundoff compensation - 10 = max number of digits for multiqueries
    if (jobDes == NULL) {
        fprintf(stderr, "init_worker_thread: unable to allocate enough memory\n");
        jobArg->error = my_strdup("init_worker_thread: unable to allocate enough memory", MYF(0));
        return 1;
    }

    //making memory larger due to invalid writes in mysql_parse at the position of the query. i have no clue why this
    //happens and this is a dirty hack
    queryCpy = (char *) my_malloc(strlen(jobArg->job->actualQuery) + 256, MYF(0));
    memset(queryCpy, 0, strlen(jobArg->job->actualQuery) + 256);
    strncpy(queryCpy, jobArg->job->actualQuery, strlen(jobArg->job->actualQuery));

    //remove any whitespace from the end of the query
    char *endOfQuery = queryCpy + strlen(queryCpy) - 1;
    while (my_isspace(jobArg->thd->charset(), *endOfQuery)) {
        *endOfQuery = '\0';
        endOfQuery--;
    }

    sprintf(jobDes, "JobWorker: U: %i P: %i Q: %s", 120, 1, queryCpy);
    thd_proc_info(jobArg->thd, jobDes);

    jobArg->thd->client_capabilities |= CLIENT_MULTI_STATEMENTS;
    jobArg->thd->set_query(queryCpy, strlen(queryCpy));

    MYSQL_QUERY_START(queryCpy, jobArg->thd->thread_id,
                      (char *) (jobArg->thd->db ? jobArg->thd->db : ""),
                      &jobArg->thd->security_ctx->priv_user[0],
                      (char *) jobArg->thd->security_ctx->host_or_ip);

    Parser_state parser_state;
    if (parser_state.init(jobArg->thd, jobArg->thd->query(), jobArg->thd->query_length())) {
        fprintf(stderr, "Query queue - job worker ERROR: error initialising parser_state object!\n");
        jobArg->error = my_strdup("Query queue - job worker ERROR: invalid query!", MYF(0));
        return 1;
    }

    jobArg->thd->init_for_queries();

    mysql_parse(jobArg->thd, jobArg->thd->query(), strlen(jobArg->thd->query()), &parser_state);

    /*
      Multiple queries exits, execute them individually
     */
    while (!jobArg->thd->killed &&
            (parser_state.m_lip.found_semicolon != NULL) &&
            !jobArg->thd->is_error()) {

        char *beginning_of_next_stmt = (char *) parser_state.m_lip.found_semicolon;

        /* Finalize server status flags after executing a statement. */
        jobArg->thd->update_server_status();
        jobArg->thd->protocol->end_statement();
        query_cache_end_of_result(jobArg->thd);
        ulong length = (ulong) strlen(jobArg->thd->query()) - (beginning_of_next_stmt - jobArg->thd->query());

        /* Remove garbage at start of query */
        while (length > 0 && my_isspace(jobArg->thd->charset(), *beginning_of_next_stmt)) {
            beginning_of_next_stmt++;
            length--;
        }

        if (length == 0)
            break;

        MYSQL_QUERY_START(beginning_of_next_stmt, jobArg->thd->thread_id,
                          (char *) (jobArg->thd->db ? jobArg->thd->db : ""),
                          &jobArg->thd->security_ctx->priv_user[0],
                          (char *) jobArg->thd->security_ctx->host_or_ip);

        jobArg->thd->set_query_and_id(beginning_of_next_stmt, length,
                                      jobArg->thd->charset(), next_query_id());
        /*
          Count each statement from the client.
         */
        statistic_increment(jobArg->thd->status_var.questions, &LOCK_status);
        parser_state.reset(beginning_of_next_stmt, length);
        mysql_parse(jobArg->thd, beginning_of_next_stmt, length, &parser_state);
    }

    if (jobArg->thd->is_error()) {

#if MYSQL_VERSION_ID >= 50603
        jobArg->error = my_strdup(jobArg->thd->get_stmt_da()->message(), MYF(0));
#else
        jobArg->error = my_strdup(jobArg->thd->stmt_da->message(), MYF(0));
#endif

#ifdef __QQUEUE_DEBUG__
        fprintf(stderr, "Query queue - job worker ERROR:\n");
        fprintf(stderr, "Error in query %i: %s\n", jobArg->job->id, jobArg->job->actualQuery);
        fprintf(stderr, "Error message: %s\n", jobArg->thd->stmt_da->message());
#endif
    }

    jobArg->thd->update_server_status();
    jobArg->thd->protocol->end_statement();
    query_cache_end_of_result(jobArg->thd);
#if MYSQL_VERSION_ID >= 50603
    jobArg->thd->get_stmt_da()->reset_diagnostics_area();
#else
    jobArg->thd->stmt_da->reset_diagnostics_area();
#endif

    my_free(queryCpy);
    my_free(jobDes);

    return 0;
}

int init_worker_thread(jobWorkerThd *job) {
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&(job->pthd), &attr, worker_thread, (void *) job) != 0) {
        fprintf(stderr, "Query queue - job worker ERROR: Could not create thread!\n");
        return 1;
    }

    pthread_detach(job->pthd);
    pthread_attr_destroy(&attr);
    return 0;
}

int registerThreadStart(jobWorkerThd *job) {
    MYSQL_TIME localTime;
    current_thd->variables.time_zone->gmt_sec_to_TIME(&localTime, (my_time_t) my_time(0));
    job->job->timeExecute = localTime;
    job->job->status = QUEUE_RUNNING;

    int error = 0;
    Open_tables_backup backup;
    TABLE *tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &backup, true, &error);
    if (error) {
        fprintf(stderr, "registerThreadStart: error in opening sys table\n");
        close_sysTbl(current_thd, tbl, &backup);
        return 1;
    }

    updateQqueueJobsRow(job->job, tbl);

    close_sysTbl(current_thd, tbl, &backup);

    return 0;
}

int registerThreadEnd(jobWorkerThd *job, bool killed, bool timedOut) {
    MYSQL_TIME localTime;
    current_thd->variables.time_zone->gmt_sec_to_TIME(&localTime, (my_time_t) my_time(0));
    job->job->timeFinish = localTime;

    if (job->error != NULL && killed == false && timedOut == false) {
        job->job->status = QUEUE_ERROR;
        strncpy(job->job->error, job->error, QQUEUE_ERROR_LEN);
    } else if (killed == true && timedOut == false) {
        job->job->status = QUEUE_KILLED;
        if (job->error != NULL) {
            strncpy(job->job->error, job->error, QQUEUE_ERROR_LEN);
        } else {
            job->job->error[0] = '\0';
        }
    } else if (timedOut == true) {
        job->job->status = QUEUE_TIMEOUT;
        if (job->error != NULL) {
            strncpy(job->job->error, job->error, QQUEUE_ERROR_LEN);
        } else {
            job->job->error[0] = '\0';
        }
    } else {
        job->job->status = QUEUE_SUCCESS;
        job->job->error[0] = '\0';
    }

    int error = 0;
    Open_tables_backup backup;
    TABLE *tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &backup, true, &error);
    if (error || (tbl == NULL && (error != HA_STATUS_NO_LOCK && error != 2) ) ) {
        fprintf(stderr, "registerThreadEnd: error in opening jobs sys table: error: %i\n", error);
        close_sysTbl(current_thd, tbl, &backup);
        return 1;
    }

    //updateQqueueJobsRow(job->job, tbl);
    deleteQqueueJobsRow(job->job->id, tbl);

    close_sysTbl(current_thd, tbl, &backup);

    tbl = open_sysTbl(current_thd, "qqueue_history", strlen("qqueue_history"), &backup, true, &error);
    if (error || tbl == NULL) {
        fprintf(stderr, "registerThreadEnd: error in opening history sys table: error: %i\n", error);
        close_sysTbl(current_thd, tbl, &backup);
        return 1;
    }

    addQqueueJobsRow(job->job, tbl, job->job->id);

    close_sysTbl(current_thd, tbl, &backup);

    return 0;
}
