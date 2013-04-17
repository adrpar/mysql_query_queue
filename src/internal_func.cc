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

int make_db_list(THD *thd, List<LEX_STRING> *files,
                 bool *with_i_schema)
{
  LEX_STRING *i_s_name_copy= 0;
  i_s_name_copy= thd->make_lex_string(i_s_name_copy,
                                      INFORMATION_SCHEMA_NAME.str,
                                      INFORMATION_SCHEMA_NAME.length, TRUE);
  *with_i_schema= 0;

  /*                                                                                                          
    Create list of existing databases. It is used in case                                                     
    of select from information schema table                                                                   
  */
  if (files->push_back(i_s_name_copy))
    return 1;
  *with_i_schema= 1;
  return (find_files(thd, files, NullS,
                     mysql_data_home, NullS, 1) != FIND_FILES_OK);
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

int make_table_name_list(THD *thd, List<LEX_STRING> *table_names,
                     bool with_i_schema, LEX_STRING *db_name)
{
  char path[FN_REFLEN + 1];
  build_table_filename(path, sizeof(path) - 1, db_name->str, "", "", 0);

  find_files_result res= find_files(thd, table_names, db_name->str, path,
                                    NULL, 0);
  if (res != FIND_FILES_OK)
  {
    return 1;
  }
  return 0;
}

