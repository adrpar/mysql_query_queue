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
 ********                  daemon_thd                      *******
 *****************************************************************
 *
 * common functions for creating and destroying threads and daemon
 * threads
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
#include <transaction.h>
#include "daemon_thd.h"

#if MYSQL_VERSION_ID >= 50606
#include <global_threads.h>
#endif

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50500
extern uint kill_one_thread(THD *thd, ulong id, killed_state kill_signal);
#endif

void pre_init_daemon_thread(THD *thd) {
    thd->client_capabilities = 0;
    thd->security_ctx->master_access = 0;
    thd->security_ctx->db_access = 0;
    thd->security_ctx->host_or_ip = (char *)my_localhost;
    my_net_init(&thd->net, NULL);
    thd->security_ctx->set_user((char *)"queue_daemon");
    thd->net.read_timeout = slave_net_timeout;
    thd->slave_thread = 0;
    thd->variables.option_bits |= OPTION_AUTO_IS_NULL;
    thd->client_capabilities |= CLIENT_MULTI_RESULTS;

#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_lock(&LOCK_thread_count);
#else
    pthread_mutex_lock(&LOCK_thread_count);
#endif
    thd->thread_id = thd->variables.pseudo_thread_id = thread_id++;
#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_unlock(&LOCK_thread_count);
#else
    pthread_mutex_unlock(&LOCK_thread_count);
#endif

    thd->proc_info = "Initialized";
    thd->set_time();

    thd->variables.lock_wait_timeout = LONG_TIMEOUT;
}

bool post_init_daemon_thread(THD *thd) {
    if (init_thr_lock() || thd->store_globals()) {
        return TRUE;
    }

#if MYSQL_VERSION_ID < 50605
    thd->start_time = my_time(0);
#else
    my_micro_time_to_timeval(my_micro_time(), &thd->start_time);
#endif


#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_lock(&LOCK_thread_count);
#else
    pthread_mutex_lock(&LOCK_thread_count);
#endif

#if MYSQL_VERSION_ID < 50606
    threads.append(thd);
    ++thread_count;
#else
    add_global_thread(thd);
#endif

#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_unlock(&LOCK_thread_count);
#else
    pthread_mutex_unlock(&LOCK_thread_count);
#endif

    return FALSE;
}


int init_thread(THD **thd, const char *threadInfo, bool daemon) {
    THD *newThd;
    my_thread_init();
    newThd = new THD;
    *thd = newThd;

#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_lock(&LOCK_thread_count);
#else
    pthread_mutex_lock(&LOCK_thread_count);
#endif
    (*thd)->thread_id = (*thd)->variables.pseudo_thread_id = thread_id++;
#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_unlock(&LOCK_thread_count);
#else
    pthread_mutex_unlock(&LOCK_thread_count);
#endif

    (*thd)->store_globals();
    (*thd)->system_thread = static_cast<enum_thread_type> (1 << 30UL);
    (*thd)->client_capabilities = 0;

    (*thd)->security_ctx->master_access = 0;
    (*thd)->security_ctx->db_access = 0;
    (*thd)->security_ctx->host_or_ip = (char *)my_localhost;
    my_net_init(&(*thd)->net, NULL);
    (*thd)->security_ctx->set_user((char *)"queue_daemon");
    (*thd)->net.read_timeout = slave_net_timeout;
    (*thd)->slave_thread = 0;
    (*thd)->variables.option_bits |= OPTION_AUTO_IS_NULL;
    (*thd)->client_capabilities |= CLIENT_MULTI_RESULTS;

    (*thd)->db = NULL;

    (*thd)->proc_info = "Initialized";
    (*thd)->set_time();

#if MYSQL_VERSION_ID < 50605
    (*thd)->start_time = my_time(0);
#else
    my_micro_time_to_timeval(my_micro_time(), &(*thd)->start_time);
#endif

    (*thd)->real_id = pthread_self();

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50500
    (*thd)->killed = NOT_KILLED;
#else
    (*thd)->killed = THD::NOT_KILLED;
#endif
#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_lock(&LOCK_thread_count);
#else
    pthread_mutex_lock(&LOCK_thread_count);
#endif

#if MYSQL_VERSION_ID < 50606
    threads.append(*thd);
    ++thread_count;
#else
    add_global_thread(*thd);
#endif

#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_unlock(&LOCK_thread_count);
#else
    pthread_mutex_unlock(&LOCK_thread_count);
#endif

    if (daemon == true) {
        (*thd)->system_thread = SYSTEM_THREAD_EVENT_SCHEDULER;
#if MYSQL_VERSION_ID < 50600
        (*thd)->command = COM_DAEMON;
#else
        (*thd)->set_command(COM_DAEMON);
#endif
    }

    return 0;
}

int deinit_thread(THD **thd) {
    (*thd)->proc_info = "Clearing";

    if (thd != NULL && *thd != NULL) {

        if (! (*thd)->in_multi_stmt_transaction_mode())
            (*thd)->mdl_context.release_transactional_locks();
        else
            (*thd)->mdl_context.release_statement_locks();

        net_end(&(*thd)->net);

#if MYSQL_VERSION_ID >= 50505
        mysql_mutex_lock(&LOCK_thread_count);
#else
        pthread_mutex_lock(&LOCK_thread_count);
#endif

#if MYSQL_VERSION_ID < 50606
        (*thd)->unlink();
        --thread_count;
#else
        remove_global_thread((*thd));
#endif

        delete (*thd);

#if MYSQL_VERSION_ID >= 50505
        mysql_cond_signal(&COND_thread_count);
        mysql_mutex_unlock(&LOCK_thread_count);
#else
        pthread_cond_signal(&COND_thread_count);
        pthread_mutex_unlock(&LOCK_thread_count);
#endif
        my_pthread_setspecific_ptr(THR_THD, 0);
    }

    *thd = NULL;

    return 0;
}

/*                                                                                                                    * kills a thread and sends response
 * (shamelessly copy pasted from mysql source sql_parse.cc)
 *
 * SYNOPSIS
 *  sql_kill()
 *  thd                 Thread class
 *  id                  Thread id
 *  only_kill_query     Should it kill the query or the connection
 */

void sql_kill(THD *thd, ulong id, bool only_kill_query) {
    uint error;
    if (!(error = kill_one_thread(thd, id, (killed_state)only_kill_query))) {
        if (!thd->killed)
            my_ok(thd);
    } else
        my_error(error, MYF(0), id);
}