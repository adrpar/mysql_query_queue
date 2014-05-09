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


#ifndef __MYSQL_EXEC_QUERY__
#define __MYSQL_EXEC_QUERY__

#define MYSQL_SERVER 1

#include <my_global.h>
#include <sql_class.h>
#include <my_pthread.h>
#include "sys_tbl.h"

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

struct jobWorkerThd {
    qqueue_jobs_row *job;
    char *error;
    int (*thdTerm)(jobWorkerThd *);
    int (*thdKillHandler)(jobWorkerThd *);
    pthread_t pthd;
    THD *thd;

    jobWorkerThd() {
        job = NULL;
        error = NULL;
        thd = NULL;
    }
};

int init_worker_thread(jobWorkerThd *job);

pthread_handler_t worker_thread(void *arg);
int workload(jobWorkerThd *jobArg);

int registerThreadStart(jobWorkerThd *job);
int registerThreadEnd(jobWorkerThd *job, bool killed, bool timedOut);

#endif