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

#ifndef __MYSQL_SQL_QUERY__
#define __MYSQL_SQL_QUERY__

#define MYSQL_SERVER 1
#include <sql_class.h>
#include <my_global.h>

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

class query_list {
public:

    query_list() {
	array = NULL;
    }

    query_list(int newLen) {
	array = (char**) my_malloc(newLen * sizeof (char), MYF(0));
	len = newLen;
    }

    virtual ~query_list() {
		if (array != NULL) {
		    my_free(array[0]);
		    my_free(array);
		}
    }

    char ** array;
    int len;
};

int addResultTableSQLAtPlaceholder(const char *inQuery, char **outQuery, char *db, char *table);
int addResultTableSQL(const char *inQuery, char **outQuery, char *db, char *table);

int validateMultiSQL(const char *inQuery);

int splitQueries(const char *inQuery, query_list **outQueryList);

#endif