/* Copyright (c) 2000, 2011, Oracle and/or its affiliates. All rights reserved.

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

/***********************************************************************
**
**  This file has the purpose to expose private functions used in querying
**  to the plugin. These functions are directly taken out of the MySQL
**  source code version 5.5.14
**
************************************************************************/


#ifndef __MYSQL_INTERNAL_FUNC__
#define __MYSQL_INTERNAL_FUNC__

#define MYSQL_SERVER 1

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
int make_db_list(THD *thd, Dynamic_array<LEX_STRING*> *files,
                 bool *with_i_schema);
#else
int make_db_list(THD *thd, List<LEX_STRING> *files,
                 bool *with_i_schema);
#endif

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
int make_table_name_list(THD *thd, Dynamic_array<LEX_STRING*> *table_names,
                         bool with_i_schema, LEX_STRING *db_name);
#else
int make_table_name_list(THD *thd, List<LEX_STRING> *table_names,
                         bool with_i_schema, LEX_STRING *db_name);
#endif

#endif