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
 ********                   sql_query                      *******
 *****************************************************************
 *
 * functions for rewriting sql query to save result in table
 *
 *****************************************************************
 */

#define MYSQL_SERVER 1

#include <stdio.h>
#include <stdlib.h>
#include <sql_list.h>
#include <ctype.h>
#include "sql_query.h"


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

#define PLACEHOLDER_STRING "/*@GEN_RES_TABLE_HERE*/"

int addResultTableSQLAtPlaceholder(const char *inQuery, char **outQuery, char *db, char *table) {
    //find the placeholder string in the query
    const char placeholderString[] = PLACEHOLDER_STRING;

    const char *plcStr;
    plcStr = strstr(inQuery, placeholderString);

    if (plcStr == NULL) {
        outQuery = NULL;
        return 0;
    }

    int prePend = plcStr - inQuery;

    //build create table select string
    int strLen = strlen(db) + strlen(table) + 5 + strlen("CREATE TABLE  ");
    char *addStr = (char *)malloc(strLen);
    if (addStr == NULL) {
        fprintf(stderr, "addResultTableSQLAtPlaceholder: unable to allocate enough memory\n");
        return 1;
    }

    sprintf(addStr, " CREATE TABLE %s.%s ", db, table);

    //build output string
    *outQuery = (char *)malloc(strlen(inQuery) + strLen);
    memset(*outQuery, 0, strlen(inQuery) + strLen);
    if (*outQuery == NULL) {
        fprintf(stderr, "addResultTableSQLAtPlaceholder: unable to allocate enough memory\n");
        return 1;
    }

    //insert create table string at right place
    strncat(*outQuery, inQuery, prePend);
    strcat(*outQuery, addStr);
    strcat(*outQuery, (char *)(inQuery + prePend + strlen(placeholderString)));

    fprintf(stderr, "Result: %s\n", *outQuery);

    free(addStr);

    return 0;
}

int addResultTableSQL(const char *inQuery, char **outQuery, char *db, char *table) {
    //create a copy of the in string
    int numTok = 0;
    char *uppCseStrCpy;
    uppCseStrCpy = (char *)malloc(strlen(inQuery) + 1);
    if (uppCseStrCpy == NULL) {
        fprintf(stderr, "addResultTableSQL: unable to allocate enough memory\n");
        return 1;
    }
    strcpy(uppCseStrCpy, inQuery);

    int i;
    for (i = 0; i < strlen(uppCseStrCpy); i++) {
        uppCseStrCpy[i] = toupper(uppCseStrCpy[i]);
    }

    //tokenize uppCseStrCpy
    List<char> tokenList;
    char *currTok;
    char *remain;
    currTok = strtok_r(uppCseStrCpy, ";", &remain);
    while (currTok != NULL) {
        tokenList.push_front(currTok);          //reorder
        currTok = strtok_r(NULL, ";", &remain);
        numTok++;
    }

    //now loop through all the sql lines from the back, to find
    //the last line with a SELECT statement and append the result field
    List_iterator<char> tokenList_iter(tokenList);
    char *currStr;
    char *selectPt;
    int prePend;
    while ( (currStr = tokenList_iter++) ) {
        selectPt = strstr(currStr, "SELECT ");
        if (selectPt != NULL) {
            prePend = selectPt - uppCseStrCpy;
            break;
        }
    }

    //build create table select string
    int strLen = strlen(db) + strlen(table) + 5 + strlen("CREATE TABLE  ");
    char *addStr = (char *)malloc(strLen);
    if (addStr == NULL) {
        fprintf(stderr, "addResultTableSQL: unable to allocate enough memory\n");
        return 1;
    }

    sprintf(addStr, "CREATE TABLE %s.%s ", db, table);

    //build output string
    *outQuery = (char *)malloc(strlen(inQuery) + strLen);
    memset(*outQuery, 0, strlen(inQuery) + strLen);
    if (*outQuery == NULL) {
        fprintf(stderr, "addResultTableSQL: unable to allocate enough memory\n");
        return 1;
    }

    //insert create table string at right place
    strncat(*outQuery, inQuery, prePend);
    strcat(*outQuery, addStr);
    strcat(*outQuery, (char *)(inQuery + prePend));

    fprintf(stderr, "Result: %s\n", *outQuery);

    free(uppCseStrCpy);
    free(addStr);

    return 0;
}

//loops though a multiline query to see, if there is no query that points into
//nirvana...
//returns 0 on success, 1 one fail
int validateMultiSQL(const char *inQuery) {
    //create a copy of the in string
    int numTok = 0;
    char *uppCseStrCpy;
    uppCseStrCpy = (char *)malloc(strlen(inQuery) + 1);
    if (uppCseStrCpy == NULL) {
        fprintf(stderr, "validateMultiSQL: unable to allocate enough memory\n");
        return 1;
    }
    strcpy(uppCseStrCpy, inQuery);

    int i;
    for (i = 0; i < strlen(uppCseStrCpy); i++) {
        uppCseStrCpy[i] = toupper(uppCseStrCpy[i]);
    }

    //check if this is a query that is managed by paqu. if yes, we don't need to check
    //anything, the query is ok
    if (strstr(uppCseStrCpy, "PAQU: QID") != NULL) {
        free(uppCseStrCpy);
        return 0;
    }

    //tokenize uppCseStrCpy
    List<char> tokenList;
    char *currTok;
    char *remain;
    currTok = strtok_r(uppCseStrCpy, ";", &remain);
    while (currTok != NULL) {
        tokenList.push_front(currTok);          //reorder
        currTok = strtok_r(NULL, ";", &remain);
        numTok++;
    }

    //now loop through all the sql lines and check if there is not a single
    //sql line, that contains a SELECT statement, without an according CREATE
    //statement. Only writing to tables here... no output to any client through NET
    List_iterator<char> tokenList_iter(tokenList);
    char *currStr;
    char *selectPt;
    char *createPt;
    while ( (currStr = tokenList_iter++) ) {
        selectPt = strstr(currStr, "SELECT ");
        if (selectPt != NULL) {
            //each select, needs to come with a create...
            createPt = strstr(currStr, "CREATE ");

            if (createPt == NULL) {
                //found a bad guy, give up and let upper routine raise alarm
                return 1;
            }

        }
    }

    free(uppCseStrCpy);

    return 0;
}

int splitQueries(const char *inQuery, query_list **outQueryList) {
    //create a copy of the in string
    int numTok = 0;
    char *queryCpy;
    queryCpy = (char *)my_malloc(strlen(inQuery), MYF(0));
    if (queryCpy == NULL) {
        fprintf(stderr, "splitQueries: unable to allocate enough memory\n");
        return 0;
    }
    strcpy(queryCpy, inQuery);

    //count and allocate
    char *currTok;
    char *remain;
    currTok = strchr(queryCpy, ';');
    while (currTok != NULL) {
        numTok++;
        currTok = strchr(currTok + 1, ';');
    }
    *outQueryList = new query_list(numTok);

    //tokenise
    currTok = strtok_r(queryCpy, ";", &remain);
    numTok = 0;
    while (currTok != NULL) {
        (*outQueryList)->array[numTok] = currTok;
        currTok = strtok_r(NULL, ";", &remain);
        numTok++;
    }

    return numTok;
}