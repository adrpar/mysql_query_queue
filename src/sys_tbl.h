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
 ********                    sys_tbl                       *******
 *****************************************************************
 * 
 * common functions for handling system tables
 * 
 *****************************************************************
 */

#ifndef __MYSQL_SYS_TBL__
#define __MYSQL_SYS_TBL__

#define MYSQL_SERVER 1

#include <sql_class.h>
#include <mysql_time.h>
#include <my_global.h>

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

#define QQUEUE_NAME_LEN 64
#define QQUEUE_RESULTDBNAME_LEN 128
#define QQUEUE_RESULTTBLNAME_LEN 128
#define QQUEUE_ERROR_LEN 1024

enum enum_queue_status {
    QUEUE_PENDING,
    QUEUE_RUNNING,
    QUEUE_DELETED,
    QUEUE_ERROR,
    QUEUE_SUCCESS,
    QUEUE_TIMEOUT,
    QUEUE_KILLED
};

#if MYSQL_VERSION_ID >= 50605
class qqueue_usrGrp_row : public ilink<qqueue_usrGrp_row> {
#else
class qqueue_usrGrp_row : public ilink {
#endif
public:
    int id;
    char name[QQUEUE_NAME_LEN];
    int priority;
};

#if MYSQL_VERSION_ID >= 50605
class qqueue_queues_row : public ilink<qqueue_queues_row> {
#else
class qqueue_queues_row : public ilink {
#endif
public:
    int id;
    char name[QQUEUE_NAME_LEN];
    int priority;
    long long timeout;
};

#if MYSQL_VERSION_ID >= 50605
class qqueue_jobs_row : public ilink<qqueue_jobs_row> {
#else
class qqueue_jobs_row : public ilink {
#endif
public:
    long long id;
    int usrId;
    int usrGroup;
    int queue;
    int priority;
    char *query;
    int queryLen;
    enum enum_queue_status status;
    char resultDBName[QQUEUE_RESULTDBNAME_LEN];
    char resultTableName[QQUEUE_RESULTTBLNAME_LEN];
    my_bool paquFlag;
    MYSQL_TIME timeSubmit;
    MYSQL_TIME timeExecute;
    MYSQL_TIME timeFinish;
    char *actualQuery;
    char error[QQUEUE_ERROR_LEN];
    char *comment;
    
    qqueue_jobs_row() {
	actualQuery = NULL;
	query = NULL;
    comment = NULL;
    }
    
    virtual ~qqueue_jobs_row() {
	if(actualQuery)
	    my_free(actualQuery);
	if(query)
	    my_free(query);
    if(comment)
        my_free(comment);
    }
};

TABLE *open_sysTbl(THD *thd, const char *tblName,
  int tblNameLen, Open_tables_backup *tblBackup,
  my_bool enableWrite, int *error);

void close_sysTbl(THD *thd, TABLE * table, Open_tables_backup *tblBackup);

int retrRowAtPKId(TABLE *table, ulonglong id);

void loadQqueueUsrGrps(TABLE *fromThisTable);
void loadQqueueQueues(TABLE *fromThisTable);
int addQqueueUsrGrpRow(qqueue_usrGrp_row *thisRow, TABLE *toThisTable);
int addQqueueQueuesRow(qqueue_queues_row *thisRow, TABLE *toThisTable);
int updateQqueueUsrGrpRow(qqueue_usrGrp_row *thisRow, TABLE *toThisTable);
int updateQqueueQueuesRow(qqueue_queues_row *thisRow, TABLE *toThisTable);

int addQqueueJobsRow(qqueue_jobs_row *thisRow, TABLE *toThisTable, ulonglong id);
int updateQqueueJobsRow(qqueue_jobs_row *thisRow, TABLE *toThisTable);
int setQqueueJobsRow(qqueue_jobs_row *thisRow, TABLE *toThisTable);
int deleteQqueueJobsRow(ulonglong id, TABLE *toThisTable);

qqueue_usrGrp_row *getUsrGrp(char * usrGrp);
qqueue_queues_row *getQueue(char * queue);
qqueue_queues_row *getQueueByID(long long id);
qqueue_jobs_row *getJobFromID(TABLE *fromThisTable, ulonglong id);
qqueue_jobs_row **getHighestPriorityJob(TABLE *fromThisTable, int numJobs);
int resetJobQueue(enum_queue_status status);
bool checkIfResultTableExists(TABLE *inThisTable, char *database, char *tblName);

#endif