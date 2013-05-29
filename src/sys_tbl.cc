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

#define MYSQL_SERVER 1

#include <stdio.h>
#include <stdlib.h>
#include <mysql_version.h>
#include <sql_class.h>
#include <sql_base.h>
#include <sql_time.h>
#include <mysql/plugin.h>
#include <mysql.h>
#include <key.h>
#include <sql_insert.h>
#include "sys_tbl.h"


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

mysql_mutex_t LOCK_usrGrps;
I_List<qqueue_usrGrp_row> usrGrps;
mysql_mutex_t LOCK_queues;
I_List<qqueue_queues_row> queues;
mysql_mutex_t LOCK_jobs;

int checkUsrGrpExisist(qqueue_usrGrp_row *thisRow);
int checkQueueExisist(qqueue_queues_row *thisRow);
void loadUsrGrps();
void loadQueues();
qqueue_jobs_row *extractJobFromTable(TABLE *fromThisTable);

struct sortElement {
    long long id;
    long long status;
    long long priority;
    MYSQL_TIME subTime;
};

int jobCmp( const void *job1, const void *job2);

TABLE *open_sysTbl(THD *thd, const char *tblName,
                   int tblNameLen, Open_tables_backup *tblBackup,
                   my_bool enableWrite, int *error) {

    TABLE *table;
    TABLE_LIST tables;

    enum thr_lock_type writeLock;

    *error = 0;

    if (enableWrite == true)
        writeLock = TL_WRITE;
    else
        writeLock = TL_READ;

    tables.init_one_table("mysql", 5, tblName, tblNameLen, tblName, writeLock);

    if (!(table = open_log_table(thd, &tables, tblBackup))) {
        *error = my_errno;
        return NULL;
    }

    table->in_use = current_thd;

    return table;
}

void close_sysTbl(THD *thd, TABLE *table, Open_tables_backup *tblBackup) {
    if (! thd->in_multi_stmt_transaction_mode())
        thd->mdl_context.release_transactional_locks();
    else
        thd->mdl_context.release_statement_locks();

    close_log_table(thd, tblBackup);
}

int retrRowAtPKId(TABLE *table, ulonglong id) {
    int error;

    table->use_all_columns();
    table->field[0]->set_notnull();
    table->field[0]->store(id, false);

    char table_key[MAX_KEY_LENGTH];
    key_copy((uchar *) table_key, table->record[0], table->key_info, table->key_info->key_length);

#if MYSQL_VERSION_ID >= 50601
    error = table->file->ha_index_read_idx_map(table->record[0], 0, (uchar *) table_key, HA_WHOLE_KEY, HA_READ_KEY_EXACT);
#else
    error = table->file->index_read_idx_map(table->record[0], 0, (uchar *) table_key, HA_WHOLE_KEY, HA_READ_KEY_EXACT);
#endif

    return error;
}

void loadQqueueUsrGrps(TABLE *fromThisTable) {
    int error;

    mysql_mutex_lock(&LOCK_usrGrps);

    usrGrps.empty();

    error = fromThisTable->file->ha_rnd_init(true);
#if MYSQL_VERSION_ID >= 50601
    while (!(error = fromThisTable->file->ha_rnd_next(fromThisTable->record[0]))) {
#else
    while (!(error = fromThisTable->file->rnd_next(fromThisTable->record[0]))) {
#endif
        String newString;
        fromThisTable->field[1]->val_str(&newString);

        qqueue_usrGrp_row *aRow = new qqueue_usrGrp_row;

        aRow->id = fromThisTable->field[0]->val_int();
        strcpy(aRow->name, newString.c_ptr());
        aRow->priority = (int) fromThisTable->field[2]->val_int();

        usrGrps.push_back(aRow);
    }

#ifdef __QQUEUE_DEBUG__
    fprintf(stderr, "loadQqueueUsrGrps: content\n");

    qqueue_usrGrp_row *aRow;
    I_List_iterator<qqueue_usrGrp_row> usrGrpIter(usrGrps);
    while (aRow = usrGrpIter++) {
        fprintf(stderr, "id: %i name: %s priority: %i\n", aRow->id, aRow->name, aRow->priority);
    }

    fprintf(stderr, "loadQqueueUsrGrps: end\n");
#endif

    mysql_mutex_unlock(&LOCK_usrGrps);
}

void loadQqueueQueues(TABLE *fromThisTable) {
    int error;

    mysql_mutex_lock(&LOCK_queues);

    queues.empty();

    error = fromThisTable->file->ha_rnd_init(true);
#if MYSQL_VERSION_ID >= 50601
    while (!(error = fromThisTable->file->ha_rnd_next(fromThisTable->record[0]))) {
#else
    while (!(error = fromThisTable->file->rnd_next(fromThisTable->record[0]))) {
#endif
        String newString;
        fromThisTable->field[1]->val_str(&newString);

        qqueue_queues_row *aRow = new qqueue_queues_row;

        aRow->id = fromThisTable->field[0]->val_int();
        strcpy(aRow->name, newString.c_ptr());
        aRow->priority = (int) fromThisTable->field[2]->val_int();
        aRow->timeout = fromThisTable->field[3]->val_int();

        queues.push_back(aRow);
    }

    fromThisTable->file->ha_rnd_end();

#ifdef __QQUEUE_DEBUG__
    fprintf(stderr, "loadQqueueQueueRow: content\n");

    qqueue_queues_row *aRow;
    I_List_iterator<qqueue_queues_row> queuesIter(queues);
    while (aRow = queuesIter++) {
        fprintf(stderr, "id: %i name: %s priority: %i timeout: %i\n", aRow->id, aRow->name,
                aRow->priority, aRow->timeout);
    }

    fprintf(stderr, "loadQqueueQueueRow: end\n");
#endif

    mysql_mutex_unlock(&LOCK_queues);
}

int addQqueueUsrGrpRow(qqueue_usrGrp_row *thisRow, TABLE *toThisTable) {
    int error;

    //try to load table if it is not yet loaded
    if (usrGrps.is_empty() == true) {
        loadUsrGrps();
    }

    toThisTable->use_all_columns();
    empty_record(toThisTable);

    if (toThisTable->next_number_field == NULL) {
        toThisTable->next_number_field = toThisTable->found_next_number_field;
    }

    toThisTable->field[0]->store(toThisTable->next_number_field->val_int(), false);

    toThisTable->field[1]->set_notnull();
    toThisTable->field[1]->store(thisRow->name, strlen(thisRow->name), system_charset_info);
    toThisTable->field[2]->set_notnull();
    toThisTable->field[2]->store(thisRow->priority, false);

    if (checkUsrGrpExisist(thisRow) == 1)
        return 1;

    //works with 1, not with 0... hmm...
    toThisTable->file->adjust_next_insert_id_after_explicit_value(toThisTable->next_number_field->val_int());

    error = toThisTable->file->ha_write_row(toThisTable->record[0]);

    if (error) {
        toThisTable->file->print_error(error, MYF(0));
        fprintf(stderr, "QQuery: Error in writing systbl qqueue_usrGrp record: %i\n", error);
        return error;
    }

    mysql_mutex_lock(&LOCK_usrGrps);
    usrGrps.push_back(thisRow);
    mysql_mutex_unlock(&LOCK_usrGrps);

    loadUsrGrps();

    return 0;
}

int addQqueueQueuesRow(qqueue_queues_row *thisRow, TABLE *toThisTable) {
    int error;

    //try to load table if it is not yet loaded
    if (queues.is_empty() == true) {
        loadQueues();
    }

    toThisTable->use_all_columns();
    empty_record(toThisTable);

    if (toThisTable->next_number_field == NULL) {
        toThisTable->next_number_field = toThisTable->found_next_number_field;
    }

    toThisTable->field[0]->store(toThisTable->next_number_field->val_int(), false);

    toThisTable->field[1]->set_notnull();
    toThisTable->field[1]->store(thisRow->name, strlen(thisRow->name), system_charset_info);
    toThisTable->field[2]->set_notnull();
    toThisTable->field[2]->store(thisRow->priority, false);
    toThisTable->field[3]->set_notnull();
    toThisTable->field[3]->store(thisRow->timeout, false);

    if (checkQueueExisist(thisRow) == 1)
        return 1;

    //works with 1, not with 0... hmm...
    toThisTable->file->adjust_next_insert_id_after_explicit_value(toThisTable->next_number_field->val_int());

    error = toThisTable->file->ha_write_row(toThisTable->record[0]);

    if (error) {
        toThisTable->file->print_error(error, MYF(0));
        fprintf(stderr, "QQuery: Error in writing systbl qqueue_queues record: %i\n", error);
        return error;
    }

    mysql_mutex_lock(&LOCK_queues);
    queues.push_back(thisRow);
    mysql_mutex_unlock(&LOCK_queues);

    loadQueues();

    return 0;
}

int addQqueueJobsRow(qqueue_jobs_row *thisRow, TABLE *toThisTable, ulonglong id) {
    int error;

    toThisTable->use_all_columns();
    empty_record(toThisTable);

    //store unique id
    toThisTable->field[0]->set_notnull();
    toThisTable->field[0]->store(id, false);

    error = setQqueueJobsRow(thisRow, toThisTable);

    if (error != 0) {
        fprintf(stderr, "QQuery: Job queue table is not correctly set up. Not the correct number of columns found.\n");
        return error;
    }


    mysql_mutex_lock(&LOCK_jobs);
    error = toThisTable->file->ha_write_row(toThisTable->record[0]);
    mysql_mutex_unlock(&LOCK_jobs);

    if (error) {
        toThisTable->file->print_error(error, MYF(0));
        fprintf(stderr, "QQuery: Error in writing systbl qqueue_jobs record: %i\n", error);
        return error;
    }

    return 0;
}

int updateQqueueJobsRow(qqueue_jobs_row *thisRow, TABLE *toThisTable) {
    int error;

    //retrieve row
    error = retrRowAtPKId(toThisTable, thisRow->id);

    if (error == HA_ERR_KEY_NOT_FOUND || error == HA_ERR_END_OF_FILE) {
        return error;
    }

    store_record(toThisTable, record[1]);
    toThisTable->use_all_columns();

    error = setQqueueJobsRow(thisRow, toThisTable);

    if (error != 0) {
        fprintf(stderr, "QQuery: Job queue table is not correctly set up. Not the correct number of columns found.\n");
        return error;
    }

    error = toThisTable->file->ha_update_row(toThisTable->record[1], toThisTable->record[0]);

    if (error && error != HA_ERR_RECORD_IS_THE_SAME) {
        toThisTable->file->print_error(error, MYF(0));
        fprintf(stderr, "QQuery: Error in updating systbl qqueue_jobs record: id: %lli error: %i\n", thisRow->id, error);
        return error;
    }
}

int setQqueueJobsRow(qqueue_jobs_row *thisRow, TABLE *toThisTable) {
    //sanity check:
    if (toThisTable->s->fields != 17) {
        return -1;
    }

    toThisTable->field[1]->set_notnull();
    if (thisRow->mysqlUserName == NULL) {
        toThisTable->field[1]->store(current_thd->security_ctx->user,
                                     strlen(current_thd->security_ctx->user),
                                     system_charset_info);
    } else {
        toThisTable->field[1]->store(thisRow->mysqlUserName, strlen(thisRow->mysqlUserName),
                                     system_charset_info);
    }

    toThisTable->field[2]->set_notnull();
    toThisTable->field[2]->store(thisRow->usrId, false);
    toThisTable->field[3]->set_notnull();
    toThisTable->field[3]->store(thisRow->usrGroup, false);
    toThisTable->field[4]->set_notnull();
    toThisTable->field[4]->store(thisRow->queue, false);
    toThisTable->field[5]->set_notnull();
    toThisTable->field[5]->store(thisRow->priority, false);
    if (thisRow->query != NULL) {
        toThisTable->field[6]->set_notnull();
        toThisTable->field[6]->store(thisRow->query, strlen(thisRow->query), system_charset_info);
    } else {
        toThisTable->field[6]->set_notnull();
        toThisTable->field[6]->store('\0', strlen('\0'), system_charset_info);
    }
    toThisTable->field[7]->set_notnull();
    toThisTable->field[7]->store(thisRow->status, false);
    toThisTable->field[8]->set_notnull();
    toThisTable->field[8]->store(thisRow->resultDBName, strlen(thisRow->resultDBName), system_charset_info);
    toThisTable->field[9]->set_notnull();
    toThisTable->field[9]->store(thisRow->resultTableName, strlen(thisRow->resultTableName), system_charset_info);
    toThisTable->field[10]->set_notnull();
    toThisTable->field[10]->store(thisRow->paquFlag, false);
    toThisTable->field[11]->set_notnull();
    toThisTable->field[11]->store_time(&thisRow->timeSubmit, MYSQL_TIMESTAMP_DATETIME);
    toThisTable->field[12]->set_notnull();
    toThisTable->field[12]->store_time(&thisRow->timeExecute, MYSQL_TIMESTAMP_DATETIME);
    toThisTable->field[13]->set_notnull();
    toThisTable->field[13]->store_time(&thisRow->timeFinish, MYSQL_TIMESTAMP_DATETIME);
    if (thisRow->actualQuery != NULL) {
        toThisTable->field[14]->set_notnull();
        toThisTable->field[14]->store(thisRow->actualQuery, strlen(thisRow->actualQuery), system_charset_info);
    } else {
        toThisTable->field[14]->set_null();
    }
    toThisTable->field[15]->set_notnull();
    toThisTable->field[15]->store(thisRow->error, strlen(thisRow->error), system_charset_info);
    if (thisRow->comment != NULL && strlen(thisRow->comment) != 0) {
        toThisTable->field[16]->set_notnull();
        toThisTable->field[16]->store(thisRow->comment, strlen(thisRow->comment), system_charset_info);
    } else {
        toThisTable->field[16]->set_null();
    }

    return 0;
}

int deleteQqueueJobsRow(ulonglong id, TABLE *toThisTable) {
    int error;

    //retrieve row
    error = retrRowAtPKId(toThisTable, id);

    if (error == HA_ERR_KEY_NOT_FOUND || error == HA_ERR_END_OF_FILE) {
        return error;
    }

    mysql_mutex_lock(&LOCK_jobs);
    error = toThisTable->file->ha_delete_row(toThisTable->record[0]);
    mysql_mutex_unlock(&LOCK_jobs);

    if (error) {
        toThisTable->file->print_error(error, MYF(0));
        fprintf(stderr, "QQuery: Error in deleting systbl qqueue_jobs record: id: %lli error: %i\n", id, error);
        return error;
    }
}

int updateQqueueUsrGrpRow(qqueue_usrGrp_row *thisRow, TABLE *toThisTable) {
    int error;

    //try to load table if it is not yet loaded
    if (usrGrps.is_empty() == true) {
        loadUsrGrps();
    }

    //retrieve row
    error = retrRowAtPKId(toThisTable, thisRow->id);

    if (error == HA_ERR_KEY_NOT_FOUND || error == HA_ERR_END_OF_FILE) {
        return error;
    }

    if (checkUsrGrpExisist(thisRow) != 1) {
#ifdef __QQUEUE_DEBUG__
        fprintf(stderr, "QQuery: updateQqueueUsrGrpRow: The row you specified does not exist\n");
#endif
        return 1;
    }

    store_record(toThisTable, record[1]);
    toThisTable->use_all_columns();

    toThisTable->field[1]->set_notnull();
    toThisTable->field[1]->store(thisRow->name, strlen(thisRow->name), system_charset_info);
    toThisTable->field[2]->set_notnull();
    toThisTable->field[2]->store(thisRow->priority, false);

    error = toThisTable->file->ha_update_row(toThisTable->record[1], toThisTable->record[0]);

    if (error && error != HA_ERR_RECORD_IS_THE_SAME) {
        toThisTable->file->print_error(error, MYF(0));
        fprintf(stderr, "QQuery: Error in updating systbl qqueue_usrGrp record: id: %i error: %i\n", thisRow->id, error);
        return error;
    }

    //reload groups list
    loadUsrGrps();
}


int updateQqueueQueuesRow(qqueue_queues_row *thisRow, TABLE *toThisTable) {
    int error;

    //try to load table if it is not yet loaded
    if (queues.is_empty() == true) {
        loadQueues();
    }

    //retrieve row
    error = retrRowAtPKId(toThisTable, thisRow->id);

    if (error == HA_ERR_KEY_NOT_FOUND || error == HA_ERR_END_OF_FILE) {
        return error;
    }

    if (checkQueueExisist(thisRow) != 1) {
#ifdef __QQUEUE_DEBUG__
        fprintf(stderr, "QQuery: updateQqueueQueuesRow: The row you specified does not exist\n");
#endif
        return 1;
    }

    store_record(toThisTable, record[1]);
    toThisTable->use_all_columns();

    toThisTable->field[1]->set_notnull();
    toThisTable->field[1]->store(thisRow->name, strlen(thisRow->name), system_charset_info);
    toThisTable->field[2]->set_notnull();
    toThisTable->field[2]->store(thisRow->priority, false);
    toThisTable->field[3]->set_notnull();
    toThisTable->field[3]->store(thisRow->timeout, false);

    error = toThisTable->file->ha_update_row(toThisTable->record[1], toThisTable->record[0]);

    if (error && error != HA_ERR_RECORD_IS_THE_SAME) {
        toThisTable->file->print_error(error, MYF(0));
        fprintf(stderr, "QQuery: Error in updating systbl qqueue_queues record: id: %i error: %i\n", thisRow->id, error);
        return error;
    }

    //reload groups list
    loadQueues();
}

int checkUsrGrpExisist(qqueue_usrGrp_row *thisRow) {
    //check if a user group with this name already exists...
    loadUsrGrps();

    if (usrGrps.is_empty() != true) {
        qqueue_usrGrp_row *aRow;
        I_List_iterator<qqueue_usrGrp_row> usrGrpIter(usrGrps);
        while (aRow = usrGrpIter++) {
            if (strcmp(aRow->name, thisRow->name) == 0) {
#ifdef __QQUEUE_DEBUG__
                fprintf(stderr, "QQuery: User group %s already exists\n", thisRow->name);
#endif
                return 1;
            }
        }
    }

    return 0;
}

int checkQueueExisist(qqueue_queues_row *thisRow) {
    //check if a queue with this name already exists...
    loadQueues();

    if (queues.is_empty() != true) {
        qqueue_queues_row *aRow;
        I_List_iterator<qqueue_queues_row> queuesIter(queues);
        while (aRow = queuesIter++) {
            if (strcmp(aRow->name, thisRow->name) == 0) {
#ifdef __QQUEUE_DEBUG__
                fprintf(stderr, "QQuery: Queue %s already exists\n", thisRow->name);
#endif
                return 1;
            }
        }
    }

    return 0;
}

void loadUsrGrps() {
    int error;
    Open_tables_backup backup;
    TABLE *tbl = open_sysTbl(current_thd, "qqueue_usrGrps", strlen("qqueue_usrGrps"), &backup, false, &error);
    loadQqueueUsrGrps(tbl);
    close_sysTbl(current_thd, tbl, &backup);
}

void loadQueues() {
    int error;
    Open_tables_backup backup;
    TABLE *tbl = open_sysTbl(current_thd, "qqueue_queues", strlen("qqueue_queues"), &backup, false, &error);
    loadQqueueQueues(tbl);
    close_sysTbl(current_thd, tbl, &backup);
}

qqueue_usrGrp_row *getUsrGrp(char *usrGrp) {
    if (usrGrps.is_empty() == true) {
        loadUsrGrps();
    }

    qqueue_usrGrp_row *aRow;
    I_List_iterator<qqueue_usrGrp_row> usrGrpIter(usrGrps);
    while (aRow = usrGrpIter++) {
        if (strcmp(aRow->name, usrGrp) == 0) {
            return aRow;
        }
    }

    return NULL;
}

qqueue_queues_row *getQueue(char *queue) {
    if (queues.is_empty() == true) {
        loadQueues();
    }

    qqueue_queues_row *aRow;
    I_List_iterator<qqueue_queues_row> queueIter(queues);
    while (aRow = queueIter++) {
        if (strcmp(aRow->name, queue) == 0) {
            return aRow;
        }
    }

    return NULL;
}

qqueue_queues_row *getQueueByID(long long id) {
    if (queues.is_empty() == true) {
        loadQueues();
    }

    qqueue_queues_row *aRow;
    I_List_iterator<qqueue_queues_row> queueIter(queues);
    while (aRow = queueIter++) {
        if (aRow->id == id) {
            return aRow;
        }
    }

    return NULL;
}

int jobCmp( const void *job1, const void *job2) {
    sortElement *j1 = (sortElement *) job1;
    sortElement *j2 = (sortElement *) job2;

    //status asc
    if (j1->status < j2->status) {
        return -1;
    } else if (j1->status > j2->status) {
        return 1;
    } else {
        //priority desc
        if (j1->priority > j2->priority) {
            return -1;
        } else if (j1->priority < j2->priority) {
            return 1;
        } else {
            //time asc
            return my_time_compare(&(j1->subTime), &(j2->subTime));
        }
    }
}

bool checkIfResultTableExists(TABLE *inThisTable, char *database, char *tblName) {
    int error;

    bool found = false;

    mysql_mutex_lock(&LOCK_jobs);
    error = inThisTable->file->ha_rnd_init(true);
#if MYSQL_VERSION_ID >= 50601
    while (!(error = inThisTable->file->ha_rnd_next(inThisTable->record[0]))) {
#else
    while (!(error = inThisTable->file->rnd_next(inThisTable->record[0]))) {
#endif
        if (inThisTable->field[6]->val_int() != QUEUE_PENDING)
            continue;

        String tmpStr;
        inThisTable->field[7]->val_str(&tmpStr);
        if (strcmp(tmpStr.c_ptr(), database) == 0) {
            String tmpStr2;
            inThisTable->field[8]->val_str(&tmpStr2);
            if (strcmp(tmpStr2.c_ptr(), tblName) == 0) {
                found = true;
                break;
            }
        }
    }
    inThisTable->file->ha_rnd_end();

    mysql_mutex_unlock(&LOCK_jobs);

    return found;
}

//this function returns a NULL terminated array of rows
//i.e. an array with numJobs+1 entries
qqueue_jobs_row **getHighestPriorityJob(TABLE *fromThisTable, int numJobs) {
    int error;

    int numTotalJobs = 0;

    mysql_mutex_lock(&LOCK_jobs);
    if (fromThisTable->file->ha_table_flags() & HA_STATS_RECORDS_IS_EXACT) {
        numTotalJobs = fromThisTable->file->stats.records;
    } else {
        //count number of rows the "stupid" way...
        error = fromThisTable->file->ha_rnd_init(true);
#if MYSQL_VERSION_ID >= 50601
        while (!(error = fromThisTable->file->ha_rnd_next(fromThisTable->record[0]))) {
#else
        while (!(error = fromThisTable->file->rnd_next(fromThisTable->record[0]))) {
#endif
            numTotalJobs++;
        }
        fromThisTable->file->ha_rnd_end();
    }

    //fprintf(stderr, "Num Rows: %i\n", numTotalJobs);

    //allocating sort buffers to do the sorting myself. couldn't find out how to do
    //this internally (i.e. select * from mysql.qqueue_jobs where status = 0 order by
    //priority desc, timeSubmit asc)

    sortElement *sortArray = (sortElement *)my_malloc(numTotalJobs * sizeof(sortElement), MYF(0));

    if (sortArray == NULL) {
        fprintf(stderr, "QQuery getHighestPrioJob: No memory to allocate sorting arrays\n");
        return NULL;
    }

    //fill arrays
    error = fromThisTable->file->ha_rnd_init(true);
    for (int i = 0; i < numTotalJobs; i++) {
#if MYSQL_VERSION_ID >= 50601
        error = fromThisTable->file->ha_rnd_next(fromThisTable->record[0]);
#else
        error = fromThisTable->file->rnd_next(fromThisTable->record[0]);
#endif

        if (error != 0)
            return NULL;

        sortArray[i].id = fromThisTable->field[0]->val_int();
        sortArray[i].status = fromThisTable->field[7]->val_int();
        sortArray[i].priority = fromThisTable->field[5]->val_int();
        fromThisTable->field[11]->get_date(&(sortArray[i].subTime), 0);
    }

    fromThisTable->file->ha_rnd_end();
    mysql_mutex_unlock(&LOCK_jobs);

#ifdef __QQUEUE_DEBUG__
    fprintf(stderr, "Qqueue jobs sort: before sorting:\n");
    for (int i = 0; i < numTotalJobs; i++) {
        fprintf(stderr, "%i %i %i %i %i %i %i %i %i\n", sortArray[i].id, sortArray[i].status,
                sortArray[i].priority, sortArray[i].subTime.day,
                sortArray[i].subTime.month, sortArray[i].subTime.year,
                sortArray[i].subTime.hour, sortArray[i].subTime.minute,
                sortArray[i].subTime.second);
    }
#endif

    //sort arrays
    my_qsort(sortArray, numTotalJobs, sizeof(sortElement), &jobCmp);

#ifdef __QQUEUE_DEBUG__
    fprintf(stderr, "Qqueue jobs sort: after sorting:\n");
    for (int i = 0; i < numTotalJobs; i++) {
        fprintf(stderr, "%i %i %i %i %i %i %i %i %i\n", sortArray[i].id, sortArray[i].status,
                sortArray[i].priority, sortArray[i].subTime.day,
                sortArray[i].subTime.month, sortArray[i].subTime.year,
                sortArray[i].subTime.hour, sortArray[i].subTime.minute,
                sortArray[i].subTime.second);
    }
#endif

    qqueue_jobs_row **result;
    result = (qqueue_jobs_row **)my_malloc((numJobs + 1) * sizeof(qqueue_jobs_row *), MYF(0));
    if (result == NULL)
        return NULL;
    memset(result, 0, (numJobs + 1) * sizeof(qqueue_jobs_row *));

    for (int i = 0; i < numJobs; i++) {
        if (i >= numTotalJobs)
            break;
        if (sortArray[i].status != 0)
            break;

        result[i] = getJobFromID(fromThisTable, sortArray[i].id);
    }

    my_free(sortArray);

    return result;
}

qqueue_jobs_row *getJobFromID(TABLE *fromThisTable, ulonglong id) {
    int err = retrRowAtPKId(fromThisTable, id);

    if (err) {
        fprintf(stderr, "QQuery: Could not find job with id: %lli. Error: %i\n", id, err);
        return NULL;
    }

    return extractJobFromTable(fromThisTable);
}

qqueue_jobs_row *extractJobFromTable(TABLE *fromThisTable) {
    qqueue_jobs_row *returnJob = new qqueue_jobs_row();

    returnJob->id = fromThisTable->field[0]->val_int();
    String tmpStr1;
    fromThisTable->field[1]->val_str(&tmpStr1);
    returnJob->mysqlUserName = my_strdup(tmpStr1.c_ptr(), MYF(0));
    returnJob->usrId = fromThisTable->field[2]->val_int();
    returnJob->usrGroup = fromThisTable->field[3]->val_int();
    returnJob->queue = fromThisTable->field[4]->val_int();
    returnJob->priority = fromThisTable->field[5]->val_int();
    String tmpStr;
    fromThisTable->field[6]->val_str(&tmpStr);
    returnJob->query = my_strdup(tmpStr.c_ptr(), MYF(0));
    returnJob->status = (enum_queue_status)fromThisTable->field[7]->val_int();
    String tmpStr2;
    fromThisTable->field[8]->val_str(&tmpStr2);
    strncpy(returnJob->resultDBName, tmpStr2.c_ptr(), QQUEUE_RESULTDBNAME_LEN);
    String tmpStr3;
    fromThisTable->field[9]->val_str(&tmpStr3);
    strncpy(returnJob->resultTableName, tmpStr3.c_ptr(), QQUEUE_RESULTTBLNAME_LEN);
    returnJob->paquFlag = fromThisTable->field[10]->val_int();
    fromThisTable->field[11]->get_date(&returnJob->timeSubmit, 0);
    fromThisTable->field[12]->get_date(&returnJob->timeExecute, 0);
    fromThisTable->field[13]->get_date(&returnJob->timeFinish, 0);
    String tmpStr4;
    fromThisTable->field[14]->val_str(&tmpStr4);
    returnJob->actualQuery = my_strdup(tmpStr4.c_ptr(), MYF(0));
    String tmpStr5;
    fromThisTable->field[15]->val_str(&tmpStr5);
    strncpy(returnJob->error, tmpStr5.c_ptr(), QQUEUE_ERROR_LEN);
    String tmpStr6;
    fromThisTable->field[16]->val_str(&tmpStr6);
    returnJob->comment = my_strdup(tmpStr6.c_ptr(), MYF(0));

    return returnJob;
}

int resetJobQueue(enum_queue_status status) {
    int error = 0;
    ulonglong *workArray = NULL;

    fprintf(stderr, "QQuery: Reseting the jobs list after restart\n");

    Open_tables_backup backup;
    TABLE *inThisJobsTable = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &backup, false, &error);
    if (error || inThisJobsTable == NULL && error != HA_STATUS_NO_LOCK) {
        fprintf(stderr, "qqueue_daemon_reset: error in opening jobs sys table: error: %i\n", error);
        close_sysTbl(current_thd, inThisJobsTable, &backup);
        return -1;
    }

    //first count the number of jobs we need to address
    int jobCount = 0;
    error = inThisJobsTable->file->ha_rnd_init(true);
#if MYSQL_VERSION_ID >= 50601
    while (!(error = inThisJobsTable->file->ha_rnd_next(inThisJobsTable->record[0]))) {
#else
    while (!(error = inThisJobsTable->file->rnd_next(inThisJobsTable->record[0]))) {
#endif
        if (inThisJobsTable->field[6]->val_int() != QUEUE_RUNNING)
            continue;

        jobCount++;
    }

    inThisJobsTable->file->ha_rnd_end();

    //allocate memory and add all the job ids we need to work with to the work array
    workArray = (ulonglong *)my_malloc(jobCount * sizeof(ulonglong), MYF(0));
    if (workArray == NULL) {
        fprintf(stderr, "QQuery: Could not allocate memory to reset job queue\n");
        return -1;
    }

    int i = 0;
    error = inThisJobsTable->file->ha_rnd_init(true);
#if MYSQL_VERSION_ID >= 50601
    while (!(error = inThisJobsTable->file->ha_rnd_next(inThisJobsTable->record[0]))) {
#else
    while (!(error = inThisJobsTable->file->rnd_next(inThisJobsTable->record[0]))) {
#endif
        if (inThisJobsTable->field[6]->val_int() != QUEUE_RUNNING)
            continue;

        workArray[i] = inThisJobsTable->field[0]->val_int();
        i++;
    }
    inThisJobsTable->file->ha_rnd_end();

    close_sysTbl(current_thd, inThisJobsTable, &backup);

    //and now do the usual magic of updating and moving the rows
    for (int i = 0; i < jobCount; i++) {
        error = 0;
        inThisJobsTable = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &backup, false, &error);
        if (error || inThisJobsTable == NULL && error != HA_STATUS_NO_LOCK) {
            fprintf(stderr, "qqueue_daemon_reset2: error in opening jobs sys table: error: %i\n", error);
            close_sysTbl(current_thd, inThisJobsTable, &backup);
            return -1;
        }

        //first get job
        qqueue_jobs_row *job = getJobFromID(inThisJobsTable, workArray[i]);
        job->status = status;

        if (status == QUEUE_ERROR) {
            //if we set running queries as errors, we need to copy them over to the
            //history table and add a meaningful error message
            char errorMessage[QQUEUE_ERROR_LEN] = "Job was canceled due to a restart of the queue and/or the server";
            strncpy(job->error, errorMessage, QQUEUE_ERROR_LEN);

            deleteQqueueJobsRow(job->id, inThisJobsTable);

            close_sysTbl(current_thd, inThisJobsTable, &backup);

            error = 0;
            Open_tables_backup backup2;
            TABLE *toThisHistoryTable = open_sysTbl(current_thd, "qqueue_history", strlen("qqueue_history"), &backup2, true, &error);
            if (error || toThisHistoryTable == NULL) {
                fprintf(stderr, "qqueue_daemon_reset: error in opening history sys table: error: %i\n", error);
                close_sysTbl(current_thd, toThisHistoryTable, &backup2);
                return 0;
            }

            addQqueueJobsRow(job, toThisHistoryTable, job->id);

            close_sysTbl(current_thd, toThisHistoryTable, &backup2);
        } else if (status == QUEUE_PENDING) {
            //if we set running queries to pending for reexecution, we just update the status and hope
            //everything is well. TODO: CHECK HERE FOR EXISTING TABLE IN FUTURE

            //cheep version of updating a row, since I could not figure out how to do this properly with
            //InnoDB... This will reset the job id though...
            deleteQqueueJobsRow(job->id, inThisJobsTable);
            addQqueueJobsRow(job, inThisJobsTable, job->id);

            close_sysTbl(current_thd, inThisJobsTable, &backup);
        } else {
            fprintf(stderr, "QQuery: Reset query: unable to reset running queries to status %i.\n", status);
            delete job;

            close_sysTbl(current_thd, inThisJobsTable, &backup);
            return -1;
        }

        delete job;
    }

    my_free(workArray);

    return jobCount;
}