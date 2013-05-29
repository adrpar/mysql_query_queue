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

#ifndef __MYSQL_DAEMON_THD__
#define __MYSQL_DAEMON_THD__

#define MYSQL_SERVER 1

#include <sql_class.h>

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif


void pre_init_daemon_thread(THD *thd);
bool post_init_daemon_thread(THD *thd);

int init_thread(THD **thd, const char *threadInfo, bool daemon);
int deinit_thread(THD **thd);
void sql_kill(THD *thd, ulong id, bool only_kill_query);

#endif