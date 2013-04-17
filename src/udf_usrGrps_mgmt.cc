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
 * *******                    UDF_QUEUE                   *******
 *****************************************************************
 * 
 * interface to the qqueue user groups functions
 * 
 *****************************************************************
 */

#define MYSQL_SERVER 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include <tztime.h>
#include <sql_parse.h>
#include "sys_tbl.h"
#include "plugin_init.h"
#include "internal_func.h"
#include "sql_query.h"
#include "exec_query.h"
#include "query_queue.h"

extern "C" {

    // user groups admin
    my_bool qqueue_addUsrGrp_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_addUsrGrp_deinit(UDF_INIT* initid);
    long long qqueue_addUsrGrp(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);

    my_bool qqueue_updateUsrGrp_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_updateUsrGrp_deinit(UDF_INIT* initid);
    long long qqueue_updateUsrGrp(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);

    my_bool qqueue_flushUsrGrps_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_flushUsrGrps_deinit(UDF_INIT* initid);
    long long qqueue_flushUsrGrps(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);

    // queues admin
    my_bool qqueue_addQueue_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_addQueue_deinit(UDF_INIT* initid);
    long long qqueue_addQueue(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);

    my_bool qqueue_updateQueue_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_updateQueue_deinit(UDF_INIT* initid);
    long long qqueue_updateQueue(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);

    my_bool qqueue_flushQueues_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_flushQueues_deinit(UDF_INIT* initid);
    long long qqueue_flushQueues(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);

    // job submission
    my_bool qqueue_addJob_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_addJob_deinit(UDF_INIT* initid);
    long long qqueue_addJob(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);
    
    my_bool qqueue_killJob_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_killJob_deinit(UDF_INIT* initid);
    long long qqueue_killJob(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);

#ifdef __QQUEUE_DEBUG__
    // job execution
    my_bool qqueue_execJob_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
    void qqueue_execJob_deinit(UDF_INIT* initid);
    long long qqueue_execJob(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error);
#endif
}

struct qqueue_table_data {
    Open_tables_backup backup;
    TABLE * tbl;
    THD * thd;
};

struct qqueue_job_data {
    Open_tables_backup backup;
    TABLE * tbl;
    qqueue_jobs_row * job;
    int priority;
    int id_usrGrp;
    int id_queue;
};

my_bool qqueue_addUsrGrp_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    //check if plugin is loaded
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }
    
    //checking stuff to be correct
    if (args->arg_count != 2) {
	strcpy(message, "wrong number of arguments: qqueue_addUsrGrp() requires two parameters");
	return 1;
    }

    if (args->arg_type[0] != STRING_RESULT) {
	strcpy(message, "qqueue_addUsrGrp() requires a string as parameter one");
	return 1;
    }

    if (args->arg_type[1] != INT_RESULT) {
	strcpy(message, "qqueue_addUsrGrp() requires an integer as parameter two");
	return 1;
    }

    int i;
    int error = 0;
    qqueue_table_data * udfData = new qqueue_table_data;
    udfData->thd = current_thd;
    udfData->tbl = open_sysTbl(udfData->thd, "qqueue_usrGrps", strlen("qqueue_usrGrps"), &udfData->backup, true, &error);
    if (error) {
	strcpy(message, "qqueue_addUsrGrp: error in opening sys table");
	return 1;
    }

    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;
    
    return 0;
}

void qqueue_addUsrGrp_deinit(UDF_INIT* initid) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    close_sysTbl(udfData->thd, udfData->tbl, &udfData->backup);
    delete (qqueue_table_data*) initid->ptr;
}

long long qqueue_addUsrGrp(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    qqueue_usrGrp_row * aRow = new qqueue_usrGrp_row();

    strcpy(aRow->name, (char*) args->args[0]);
    aRow->priority = *(long long*) args->args[1];

    int error = addQqueueUsrGrpRow(aRow, udfData->tbl);

    delete aRow;

    return error;
}

my_bool qqueue_updateUsrGrp_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }

    //checking stuff to be correct
    if (args->arg_count != 3) {
	strcpy(message, "wrong number of arguments: qqueue_updateUsrGrp() requires three parameters");
	return 1;
    }

    if (args->arg_type[0] != INT_RESULT) {
	strcpy(message, "qqueue_updateUsrGrp() requires an integer as parameter one");
	return 1;
    }

    if (args->arg_type[1] != STRING_RESULT) {
	strcpy(message, "qqueue_updateUsrGrp() requires a string as parameter two");
	return 1;
    }

    if (args->arg_type[2] != INT_RESULT) {
	strcpy(message, "qqueue_updateUsrGrp() requires an integer as parameter three");
	return 1;
    }

    int i;
    int error = 0;
    qqueue_table_data * udfData = new qqueue_table_data;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_usrGrps", strlen("qqueue_usrGrps"), &udfData->backup, true, &error);
    if (error) {
	strcpy(message, "qqueue_updateUsrGrp: error in opening sys table");
	return 1;
    }

    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;

    return 0;
}

void qqueue_updateUsrGrp_deinit(UDF_INIT* initid) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    delete (qqueue_table_data*) initid->ptr;
}

long long qqueue_updateUsrGrp(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    qqueue_usrGrp_row * aRow = new qqueue_usrGrp_row();

    aRow->id = *(long long*) args->args[0];
    strcpy(aRow->name, (char*) args->args[1]);
    aRow->priority = *(long long*) args->args[2];

    int error = updateQqueueUsrGrpRow(aRow, udfData->tbl);

    delete aRow;

    return error;
}


my_bool qqueue_flushUsrGrps_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }

    //checking stuff to be correct
    if (args->arg_count != 0) {
	strcpy(message, "wrong number of arguments: qqueue_flushUsrGrp() requires no parameter");
	return 1;
    }

    int i;
    int error = 0;
    qqueue_table_data * udfData = new qqueue_table_data;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_usrGrps", strlen("qqueue_usrGrps"), &udfData->backup, false, &error);
    if (error) {
	strcpy(message, "qqueue_flushUsrGrp: error in opening sys table");
	return 1;
    }

    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;

    return 0;    
}

void qqueue_flushUsrGrps_deinit(UDF_INIT* initid) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    delete (qqueue_table_data*) initid->ptr;
}

long long qqueue_flushUsrGrps(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;

    loadQqueueUsrGrps(udfData->tbl);

    return 0;
}


////////////////////////////////////////////////////////////////////////////////
///// queues function implementation ///////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

my_bool qqueue_addQueue_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }

    //checking stuff to be correct
    if (args->arg_count != 3) {
	strcpy(message, "wrong number of arguments: qqueue_addQueue() requires three parameters");
	return 1;
    }

    if (args->arg_type[0] != STRING_RESULT) {
	strcpy(message, "qqueue_addQueue() requires a string as parameter one");
	return 1;
    }

    if (args->arg_type[1] != INT_RESULT) {
	strcpy(message, "qqueue_addQueue() requires an integer as parameter two");
	return 1;
    }

    if (args->arg_type[2] != INT_RESULT) {
	strcpy(message, "qqueue_addQueue() requires an integer as parameter three");
	return 1;
    }

    int i;
    int error = 0;
    qqueue_table_data * udfData = new qqueue_table_data;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_queues", strlen("qqueue_queues"), &udfData->backup, true, &error);
    if (error) {
	strcpy(message, "qqueue_addQueue: error in opening sys table");
	return 1;
    }

    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;

    return 0;
}

void qqueue_addQueue_deinit(UDF_INIT* initid) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    delete (qqueue_table_data*) initid->ptr;
}

long long qqueue_addQueue(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    qqueue_queues_row * aRow = new qqueue_queues_row();

    strcpy(aRow->name, (char*) args->args[0]);
    aRow->priority = *(long long*) args->args[1];
    aRow->timeout = *(long long*) args->args[2];

    int error = addQqueueQueuesRow(aRow, udfData->tbl);

    delete aRow;

    return error;
}

my_bool qqueue_updateQueue_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }

    //checking stuff to be correct
    if (args->arg_count != 4) {
	strcpy(message, "wrong number of arguments: qqueue_updateQueue() requires four parameters");
	return 1;
    }

    if (args->arg_type[0] != INT_RESULT) {
	strcpy(message, "qqueue_updateQueue() requires an integer as parameter one");
	return 1;
    }

    if (args->arg_type[1] != STRING_RESULT) {
	strcpy(message, "qqueue_updateQueue() requires a string as parameter two");
	return 1;
    }

    if (args->arg_type[2] != INT_RESULT) {
	strcpy(message, "qqueue_updateQueue() requires an integer as parameter three");
	return 1;
    }

    if (args->arg_type[3] != INT_RESULT) {
	strcpy(message, "qqueue_updateQueue() requires an integer as parameter four");
	return 1;
    }

    int i;
    int error = 0;
    qqueue_table_data * udfData = new qqueue_table_data;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_queues", strlen("qqueue_queues"), &udfData->backup, true, &error);
    if (error) {
	strcpy(message, "qqueue_updateQueue: error in opening sys table");
	return 1;
    }

    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;

    return 0;
}

void qqueue_updateQueue_deinit(UDF_INIT* initid) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    delete (qqueue_table_data*) initid->ptr;
}

long long qqueue_updateQueue(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    qqueue_queues_row * aRow = new qqueue_queues_row();

    aRow->id = *(long long*) args->args[0];
    strcpy(aRow->name, (char*) args->args[1]);
    aRow->priority = *(long long*) args->args[2];
    aRow->timeout = *(long long*) args->args[3];

    int error = updateQqueueQueuesRow(aRow, udfData->tbl);

    delete aRow;

    return error;
}


my_bool qqueue_flushQueues_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }

    //checking stuff to be correct
    if (args->arg_count != 0) {
	strcpy(message, "wrong number of arguments: qqueue_flushQueues() requires no parameter");
	return 1;
    }

    int i;
    int error = 0;
    qqueue_table_data * udfData = new qqueue_table_data;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_queues", strlen("qqueue_queues"), &udfData->backup, false, &error);
    if (error) {
	strcpy(message, "qqueue_queues: error in opening sys table");
	return 1;
    }

    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;

    return 0;    
}

void qqueue_flushQueues_deinit(UDF_INIT* initid) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    delete (qqueue_table_data*) initid->ptr;
}

long long qqueue_flushQueues(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_table_data * udfData = (qqueue_table_data*) initid->ptr;

    loadQqueueQueues(udfData->tbl);

    return 0;
}

////////////////////////////////////////////////////////////////////////////////
///// jobsub function implementation ///////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

my_bool qqueue_addJob_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }

    //checking stuff to be correct
    if (!(args->arg_count == 8 || args->arg_count == 9)) {
	strcpy(message, "wrong number of arguments: qqueue_addJob() requires seven (if actual query is given with paqu flag on, eight) parameters");
	return 1;
    }

    if (args->arg_type[0] != INT_RESULT) {
	strcpy(message, "qqueue_addJob() requires an integer as parameter one");
	return 1;
    }

    if (args->arg_type[1] != STRING_RESULT) {
	strcpy(message, "qqueue_addJob() requires an string as parameter two");
	return 1;
    }

    if (args->arg_type[2] != STRING_RESULT) {
	strcpy(message, "qqueue_addJob() requires an string as parameter three");
	return 1;
    }

    if (args->arg_type[3] != STRING_RESULT) {
	strcpy(message, "qqueue_addJob() requires a string as parameter four");
	return 1;
    }

    if (args->arg_type[4] != STRING_RESULT) {
	strcpy(message, "qqueue_addJob() requires a string as parameter five");
	return 1;
    }

    if (args->arg_type[5] != STRING_RESULT) {
	strcpy(message, "qqueue_addJob() requires a string as parameter six");
	return 1;
    }

    if (args->arg_type[6] != STRING_RESULT) {
    strcpy(message, "qqueue_addJob() requires a string as parameter seven");
    return 1;
    }

    if (args->arg_type[7] != INT_RESULT) {
	strcpy(message, "qqueue_addJob() requires an integer as parameter eight");
	return 1;
    }

    if (args->arg_count == 9) {
        if (args->arg_type[8] != STRING_RESULT) {
        strcpy(message, "qqueue_addJob() requires an string as parameter nine");
        return 1;
        }

        //sanity check
        if (*(int*)args->args[7] != 1) {
        strcpy(message, "qqueue_addJob(): actual query can only be given when paqu flag is set to 1!");
        return 1;
        }
    }

    //retrieve and check userGrp and queue for priority calculation
    qqueue_usrGrp_row *priority_usrGrp = getUsrGrp((char*) args->args[1]);
    qqueue_queues_row *priority_queue = getQueue((char*) args->args[2]);
    
    if (priority_usrGrp == NULL) {
	strcpy(message, "qqueue_addJob() user group not found");
	return 1;
    }
    
    if (priority_queue == NULL) {
	strcpy(message, "qqueue_addJob() queue not found");
	return 1;
    }
    
    //obtain list of all databases
    List<LEX_STRING> db_names;
    bool with_i_schema;
    if(make_db_list(current_thd, &db_names, &with_i_schema)) {
	strcpy(message, "qqueue_addJob() could not retrieve list of databases");
	return 1;
    }
    
    List_iterator<LEX_STRING> db_names_iter(db_names);
    LEX_STRING * currDBStr;
    bool found = 0;
    while (currDBStr = db_names_iter++) {
	if(strcmp((char*)args->args[4], currDBStr->str) == 0) {
	    found = 1;
	    break;
	}
    }
    
    if(found == 0) {
	strcpy(message, "qqueue_addJob() could not find the result database you specified.");
	return 1;
    }
    

    //check if the table exists in the database
    List<LEX_STRING> table_names;
    if(make_table_name_list(current_thd, &table_names, with_i_schema, currDBStr)) {
	strcpy(message, "qqueue_addJob() could not retrieve list of tables for specified database");
	return 1;
    }
    
    List_iterator<LEX_STRING> tbl_names_iter(table_names);
    LEX_STRING * currTblStr;
    found = 0;
    while (currTblStr = tbl_names_iter++) {
	if(strcmp((char*)args->args[5], currTblStr->str) == 0) {
	    found = 1;
	    break;
	}
    }
    
    if(found == 1) {
	strcpy(message, "qqueue_addJob() the result table already exists.");
	return 1;
    }
    
    int i;
    int error = 0;
    qqueue_job_data * udfData = new qqueue_job_data;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &udfData->backup, true, &error);
    if (error) {
    	strcpy(message, "qqueue_addJob: error in opening sys table");
        close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    	delete udfData;
        return 1;
    }

    if(checkIfResultTableExists(udfData->tbl, (char*)args->args[4], (char*)args->args[5]) == true) {
    	strcpy(message, "qqueue_addJob() the result table will already be created by another query in the queue.");
        close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
        delete udfData;
    	return 1;
    }
    
    udfData->job = new qqueue_jobs_row();

    //process sql here and raise error if there is an issue. remaining stuff will be dealt with later
    //we need to check if there are multiple SELECT statements that are not balanced with a CREATE TABLE
    //here... otherwise MySQL would segfault spectacularly. 
    char * outQuery = NULL;

    //if the query was processed by PaQu, then the result table does not need to be added...
    if(*(long long*) args->args[7] != 1) {
       addResultTableSQLAtPlaceholder((char*) args->args[3], &outQuery, (char*) args->args[4], (char*) args->args[5]);
       if(outQuery == NULL)
           addResultTableSQL((char*) args->args[3], &outQuery, (char*) args->args[4], (char*) args->args[5]);
    }

    if(outQuery == NULL) {
       udfData->job->actualQuery = my_strdup((char*) args->args[3], MYF(0));
    } else {
       udfData->job->actualQuery = my_strdup(outQuery, MYF(0));
       free(outQuery);
    }

    //check if there are no wild SELECT statements in here...
    if(validateMultiSQL(udfData->job->actualQuery) != 0) {
        strcpy(message, "qqueue_addJob() there are multiple SELECT statments not captured by CREATE TABLE!");
        close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
        delete udfData->job;
        delete udfData;
        return 1;
    }

    udfData->priority = priority_usrGrp->priority * priority_queue->priority;
    udfData->id_usrGrp = priority_usrGrp->id;
    udfData->id_queue = priority_queue->id;

    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;
    
    return 0;
}

void qqueue_addJob_deinit(UDF_INIT* initid) {
    qqueue_job_data * udfData = (qqueue_job_data*) initid->ptr;
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    delete (qqueue_job_data*) initid->ptr;
}

long long qqueue_addJob(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_job_data * udfData = (qqueue_job_data*) initid->ptr;
    qqueue_jobs_row * aRow = udfData->job;
    
    //calculate unique (hopefully) id for this job
    ulonglong currTimeID;
    currTimeID = my_micro_time();
    //shift by 8 bits to make some space for a random component
    currTimeID = currTimeID << 8;
    //add the last 8 bits of a random number to the id
    currTimeID += (ulonglong)(sql_rnd_with_mutex() & 0x000000ff);

    aRow->id = currTimeID;
    aRow->usrId = *(long long*) args->args[0];
    aRow->usrGroup = udfData->id_usrGrp;
    aRow->queue = udfData->id_queue;
    aRow->priority = udfData->priority;
    if(args->arg_count == 9) {
        aRow->query = my_strdup((char*) args->args[8], MYF(0));
    } else {
        aRow->query = my_strdup((char*) args->args[3], MYF(0));
    }

    aRow->status = QUEUE_PENDING;
    strcpy(aRow->resultDBName, (char*) args->args[4]);
    strcpy(aRow->resultTableName, (char*) args->args[5]);
    if(args->args[6] != NULL) {
        aRow->comment = my_strdup((char*) args->args[6], MYF(0));
    } else {
        aRow->comment = NULL;
    }
    aRow->paquFlag = *(long long*) args->args[7];
    
    MYSQL_TIME localTime;
    current_thd->variables.time_zone->gmt_sec_to_TIME(&localTime, (my_time_t) my_time(0));
    aRow->timeSubmit = localTime;
    MYSQL_TIME nullTime = {0, 0, 0, 0, 0, 0, 0, 0};
    aRow->timeExecute = nullTime;
    aRow->timeFinish = nullTime,
    strcpy(aRow->error, "");
    
    int err = addQqueueJobsRow(aRow, udfData->tbl, currTimeID);

    delete udfData->job;

    return err;
}

////////////////////////////////////////////////////////////////////////////////
///// delete/kill job implementation        ////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

my_bool qqueue_killJob_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }

    //checking stuff to be correct
    if (args->arg_count != 1) {
	strcpy(message, "wrong number of arguments: qqueue_killJob() requires one parameter");
	return 1;
    }

    if (args->arg_type[0] != INT_RESULT) {
	strcpy(message, "qqueue_killJob() requires an integer as parameter one");
	return 1;
    }

    
    int i;
    int error = 0;
    qqueue_job_data * udfData = new qqueue_job_data;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &udfData->backup, true, &error);
    if (error) {
	strcpy(message, "qqueue_killJob: error in opening sys table");
	return 1;
    }

    //retrieve job from the jobs table and if it doesn't exist, throw error
    udfData->job = getJobFromID(udfData->tbl, *(long long*)args->args[0]);
    
    if(udfData->job == NULL) {
	strcpy(message, "qqueue_killJob: cannot find the job you specified");
	return 1;
    }
    
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    
    //check if we should run this query
    if(udfData->job->status != QUEUE_PENDING && udfData->job->status != QUEUE_RUNNING) {
	strcpy(message, "qqueue_killJob: this job is not pending or running... therefore I cannot delete...");
	return 1;
    }
    
    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;
    
    return 0;
}

void qqueue_killJob_deinit(UDF_INIT* initid) {
    qqueue_job_data * udfData = (qqueue_job_data*) initid->ptr;
    delete ((qqueue_job_data*) initid->ptr)->job;
    delete (qqueue_job_data*) initid->ptr;
}

long long qqueue_killJob(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_job_data * udfData = (qqueue_job_data*) initid->ptr;
    
    lockQueue();
    
    int error;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &udfData->backup, true, &error);
    if (error) {
	unlockQueue();
	return -1 * error;
    }

    //retrieve job from the jobs table and if it doesn't exist, throw error
    qqueue_jobs_row *row = getJobFromID(udfData->tbl, *(long long*)args->args[0]);

    if(row == NULL) {
	unlockQueue();
	return -1;
    }
    
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    
    if(row->status == QUEUE_PENDING) {
	MYSQL_TIME localTime;
	current_thd->variables.time_zone->gmt_sec_to_TIME(&localTime, (my_time_t) my_time(0));
	row->timeFinish = localTime;
    
	row->status = QUEUE_DELETED;
	row->error[0] = '\0';

	Open_tables_backup backup;
	TABLE * tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &backup, true, &error);
	if (error || tbl == NULL && error != HA_STATUS_NO_LOCK) {
		fprintf(stderr, "qqueue_killJob: error in opening jobs sys table\n");
		close_sysTbl(current_thd, udfData->tbl, &backup);
		return 1;
	}

	deleteQqueueJobsRow(*(long long*)args->args[0], tbl);
    
	close_sysTbl(current_thd, udfData->tbl, &backup);
    
	tbl = open_sysTbl(current_thd, "qqueue_history", strlen("qqueue_history"), &backup, true, &error);
	if (error) {
	    fprintf(stderr, "qqueue_killJob: error in opening history sys table\n");
	    close_sysTbl(current_thd, udfData->tbl, &backup);
	    return 1;
	}

	addQqueueJobsRow(row, tbl, row->id);
    
	close_sysTbl(current_thd, udfData->tbl, &backup);

    } else if (row->status == QUEUE_RUNNING) {
	registerJobKill(*(long long*)args->args[0]);
    }
    
    unlockQueue();
    
    return 0;
}


#ifdef __QQUEUE_DEBUG__
////////////////////////////////////////////////////////////////////////////////
///// execute job prototyping        ///////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

my_bool qqueue_execJob_init(UDF_INIT* initid, UDF_ARGS* args, char* message) {
    if(getPluginInstalled() == 0) {
	strcpy(message, "Qqueue pluing is not installed on this MySQL instance.");
	return 1;
    }

    //checking stuff to be correct
    if (args->arg_count != 1) {
	strcpy(message, "wrong number of arguments: qqueue_execJob() requires one parameter");
	return 1;
    }

    if (args->arg_type[0] != INT_RESULT) {
	strcpy(message, "qqueue_execJob() requires an integer as parameter one");
	return 1;
    }

    
    int i;
    int error = 0;
    qqueue_job_data * udfData = new qqueue_job_data;
    udfData->tbl = open_sysTbl(current_thd, "qqueue_jobs", strlen("qqueue_jobs"), &udfData->backup, true, &error);
    if (error) {
	strcpy(message, "qqueue_execJob: error in opening sys table");
	return 1;
    }

    //retrieve job from the jobs table and if it doesn't exist, throw error
    udfData->job = getJobFromID(udfData->tbl, *(long long*)args->args[0]);
    
    if(udfData->job == NULL) {
	strcpy(message, "qqueue_execJob: cannot find the job you specified");
	return 1;
    }
    
    close_sysTbl(current_thd, udfData->tbl, &udfData->backup);
    
    //check if we should run this query
    if(udfData->job->status != QUEUE_PENDING) {
	strcpy(message, "qqueue_execJob: this job is not pending... not keen on running it again...");
	return 1;
    }
    
    //no limits on number of decimals
    initid->decimals = 31;
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*) udfData;
    
    return 0;
}

void qqueue_execJob_deinit(UDF_INIT* initid) {
    qqueue_job_data * udfData = (qqueue_job_data*) initid->ptr;
    delete (qqueue_job_data*) initid->ptr;
}

long long qqueue_execJob(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error) {
    qqueue_job_data * udfData = (qqueue_job_data*) initid->ptr;
    
    jobWorkerThd *job = new jobWorkerThd();
    job->job = udfData->job;
    //job->thdTerm = registerThreadEnd;
    job->thdTerm = NULL;

    //register start of execution
    registerThreadStart(job);
    
    init_worker_thread(job);
    
    //char query[] = "select * from mysql.qqueue_jobs; insert into tmp.tmp values (1, 2); delete from mysql.qqueue_jobs; insert into tmp.tmp values (1, 2);";
    //char query[] = "insert into tmp.tmp values (1, 2); insert into tmp.tmp values (1, 2);";
    
    return 0;
}
#endif