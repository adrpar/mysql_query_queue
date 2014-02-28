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


#define MYSQL_SERVER 1

#include <stdio.h>
#include <stdlib.h>
#include <mysql_version.h>
#include <sql_class.h>
#include <sql_base.h>
#include <mysql/plugin.h>
#include <mysql.h>
#include <key.h>
#include <sql_insert.h>
#include <sql_show.h>
#include <sql_table.h>
#include "internal_func.h"


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000

enum find_files_result {
  FIND_FILES_OK,
  FIND_FILES_OOM,
  FIND_FILES_DIR
};

/** Hash of LEX_STRINGs used to search for ignored db directories. */
static HASH ignore_db_dirs_hash;

/**
  Check if a directory name is in the hash of ignored directories.

  @return search result
  @retval TRUE  found
  @retval FALSE not found
*/

static inline bool
is_in_ignore_db_dirs_list(const char *directory)
{
  return ignore_db_dirs_hash.records &&
    NULL != my_hash_search(&ignore_db_dirs_hash, (const uchar *) directory, 
                           strlen(directory));
}

/*
  find_files() - find files in a given directory.

  SYNOPSIS
    find_files()
    thd                 thread handler
    files               put found files in this list
    db                  database name to search tables in
                        or NULL to search for databases
    path                path to database
    wild                filter for found files

  RETURN
    FIND_FILES_OK       success
    FIND_FILES_OOM      out of memory error
    FIND_FILES_DIR      no such directory, or directory can't be read
*/


static find_files_result
find_files(THD *thd, Dynamic_array<LEX_STRING*> *files, LEX_STRING *db,
           const char *path, const LEX_STRING *wild)
{
  MY_DIR *dirp;
  Discovered_table_list tl(thd, files, wild);
  DBUG_ENTER("find_files");

  if (!(dirp = my_dir(path, MY_THREAD_SPECIFIC | (db ? 0 : MY_WANT_STAT))))
  {
    if (my_errno == ENOENT)
      my_error(ER_BAD_DB_ERROR, MYF(ME_BELL | ME_WAITTANG), db->str);
    else
      my_error(ER_CANT_READ_DIR, MYF(ME_BELL | ME_WAITTANG), path, my_errno);
    DBUG_RETURN(FIND_FILES_DIR);
  }

  if (!db)                                           /* Return databases */
  {
    for (uint i=0; i < (uint) dirp->number_of_files; i++)
    {
      FILEINFO *file= dirp->dir_entry+i;
#ifdef USE_SYMDIR
      char *ext;
      char buff[FN_REFLEN];
      if (my_use_symdir && !strcmp(ext=fn_ext(file->name), ".sym"))
      {
        /* Only show the sym file if it points to a directory */
        char *end;
        *ext=0;                                 /* Remove extension */
        unpack_dirname(buff, file->name);
        end= strend(buff);
        if (end != buff && end[-1] == FN_LIBCHAR)
          end[-1]= 0;       // Remove end FN_LIBCHAR
        if (!mysql_file_stat(key_file_misc, buff, file->mystat, MYF(0)))
               continue;
       }
#endif
      if (!MY_S_ISDIR(file->mystat->st_mode))
        continue;

      if (is_in_ignore_db_dirs_list(file->name))
        continue;

      if (tl.add_file(file->name))
        goto err;
    }
    tl.sort();
  }
  else
  {
    if (ha_discover_table_names(thd, db, dirp, &tl, false))
      goto err;
  }

  DBUG_PRINT("info",("found: %zu files", files->elements()));
  my_dirend(dirp);

  DBUG_RETURN(FIND_FILES_OK);

err:
  my_dirend(dirp);
  DBUG_RETURN(FIND_FILES_OOM);
}


#endif

/*
  Create db names list. Information schema name always is first in list

  SYNOPSIS
    make_db_list()
    thd                   thread handler
    files                 list of db names
    wild                  wild string
    idx_field_vals        idx_field_vals->db_name contains db name or
                          wild string
    with_i_schema         returns 1 if we added 'IS' name to list
                          otherwise returns 0

  RETURN
    zero                  success
    non-zero              error
*/

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
int make_db_list(THD *thd, Dynamic_array<LEX_STRING*> *files,
                 bool *with_i_schema) {
#else
int make_db_list(THD *thd, List<LEX_STRING> *files,
                 bool *with_i_schema) {
#endif
    LEX_STRING *i_s_name_copy = 0;
    
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    i_s_name_copy = thd->make_lex_string(INFORMATION_SCHEMA_NAME.str,
                                         INFORMATION_SCHEMA_NAME.length);
#else
    i_s_name_copy = thd->make_lex_string(i_s_name_copy,
                                         INFORMATION_SCHEMA_NAME.str,
                                         INFORMATION_SCHEMA_NAME.length, TRUE);
#endif
    *with_i_schema = 0;

    /*
      Create list of existing databases. It is used in case
      of select from information schema table
    */
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    if (files->append_val(i_s_name_copy))
#else
    if (files->push_back(i_s_name_copy))
#endif
        return 1;
    *with_i_schema = 1;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    return (find_files(thd, files, 0, mysql_data_home, &null_lex_str) != FIND_FILES_OK);    
#else
    return (find_files(thd, files, NullS,
                       mysql_data_home, NullS, 1) != FIND_FILES_OK);
#endif
}

/**
  @brief          Create table names list

  @details        The function creates the list of table names in
                  database

  @param[in]      thd                   thread handler
  @param[in]      table_names           List of table names in database
  @param[in]      lex                   pointer to LEX struct
  @param[in]      lookup_field_vals     pointer to LOOKUP_FIELD_VALUE struct
  @param[in]      with_i_schema         TRUE means that we add I_S tables to list
  @param[in]      db_name               database name

  @return         Operation status
    @retval       0           ok
    @retval       1           fatal error
    @retval       2           Not fatal error; Safe to ignore this file list
*/

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
int make_table_name_list(THD *thd, Dynamic_array<LEX_STRING*> *table_names,
                         bool with_i_schema, LEX_STRING *db_name) {
#else
int make_table_name_list(THD *thd, List<LEX_STRING> *table_names,
                         bool with_i_schema, LEX_STRING *db_name) {
#endif
    char path[FN_REFLEN + 1];
    build_table_filename(path, sizeof(path) - 1, db_name->str, "", "", 0);

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    find_files_result res= find_files(thd, table_names, db_name, path,
                                       &null_lex_str);
#else    
    find_files_result res = find_files(thd, table_names, db_name->str, path,
                                       NULL, 0);
#endif

    if (res != FIND_FILES_OK) {
        return 1;
    }
    return 0;
}

