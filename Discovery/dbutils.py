## -*- coding: utf-8 -*-

# Discovery Plugin
#
# Copyright (C) 2015 Lutra Consulting
# info@lutraconsulting.co.uk
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from PyQt4.QtCore import *

# import psycopg2
import pyodbc


def get_connection(conn_info):
    """ Connect to the database using conn_info dict:
     { 'host': ..., 'port': ..., 'database': ..., 'username': ..., 'password': ... }
    """
    conn = pyodbc.connect(**conn_info)
    #conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def get_sqlserver_connections():
    """ Read SQL Server connection names from QSettings stored by QGIS
    """
    settings = QSettings()
    settings.beginGroup(u"/MSSQL/connections/")
    return settings.childGroups()


"""
def current_postgres_connection():
    settings = QSettings()
    settings.beginGroup("/Discovery")
    return settings.value("connection", "", type=str)
"""

# Work on this function
def get_sqlserver_conn_info(selected):
    """ Read SQL Server connection details from QSettings stored by QGIS
    """
    settings = QSettings()
    settings.beginGroup(u"/MSSQL/connections/" + selected)
    if not settings.contains("database"): # non-existent entry?
        return {}

    conn_info = {}
    conn_info["host"] = settings.value("host", "", type=str)
    conn_info["port"] = settings.value("port", 1433, type=int)
    conn_info["database"] = settings.value("database", "", type=str)
    username = settings.value("username", "", type=str)
    password = settings.value("password", "", type=str)
    if len(username) != 0:
        conn_info["user"] = username
        conn_info["password"] = password
    return conn_info


def _quote(identifier):
    """ quote identifier """
    return u'"%s"' % identifier.replace('"', '""')

def _quote_str(txt):
    """ make the string safe - replace ' with '' """
    return txt.replace("'", "''")


def list_schemas(in_cursor):
    """ Get list of schema names
    	Only pulling out dbo schema
   
    """
    schemas = list(set([r.table_schem for r in in_cursor.tables().fetchall() if r.table_schem=='dbo'])
    if schemas:
        return sorted(schemas)

                   
def list_tables(in_cursor, in_schema,in_tabletype='TABLE'):
    """
    Returns list of all tables(spatial and non spatial)
    """
    tables = [t.table_name for t in in_cur.tables(schema=in_schema, tableType=in_tableType).fetchall()]
	if tables is not None:
		return sorted(tables)
    
                   
def list_columns(in_cursor, in_schema, in_table):
    """
    Returns list of columns of a particular table.
    """
    columns = [c.column_name for c in in_cursor.columns(table=in_table,schema=in_schema).fetchall()]
    return sorted(columns)


def get_search_sql(search_text, geom_column, search_column, echo_search_column, display_columns, extra_expr_columns, schema, table):
    """ Returns a tuple: (SQL query text, dictionary with values to replace variables with).
    """

    """
    Spaces in queries
        A query with spaces is executed as follows:
            'my query'
            ILIKE '%my%query%'

    A note on spaces in postcodes
        Postcodes must be stored in the DB without spaces:
            'DL10 4DQ' becomes 'DL104DQ'
        This allows users to query with or without spaces
        As wildcards are inserted at spaces, it doesn't matter whether the query is:
            'dl10 4dq'; or
            'dl104dq'
    """

    wildcarded_search_string = ''
    for part in search_text.split():
        wildcarded_search_string += '%' + part
    wildcarded_search_string += '%'
    query_dict = {'search_text': wildcarded_search_string}

    query_text = """ SELECT
                        STAsText("%s") AS geom,
                        STSRID("%s") AS epsg,
                 """ % (geom_column, geom_column)
    if echo_search_column:
        query_column_selection_text = """"%s"
                                      """ % search_column
        suggestion_string_seperator = ', '
    else:
        query_column_selection_text = """''"""
        suggestion_string_seperator = ''
    if len(display_columns) > 0:
        for display_column in display_columns.split(','):
            query_column_selection_text += """ || CASE WHEN "%s" IS NOT NULL THEN
                                                     '%s' || "%s"
                                                 ELSE
                                                     ''
                                                 END
                                           """ % (display_column, suggestion_string_seperator, display_column)
            suggestion_string_seperator = ', '
    query_column_selection_text += """ AS suggestion_string """
    if query_column_selection_text.startswith("'', "):
        query_column_selection_text = query_column_selection_text[4:]
    query_text += query_column_selection_text
    for extra_column in extra_expr_columns:
        query_text += ', "%s"' % extra_column
    query_text += """
                  FROM
                        "%s"."%s"
                     WHERE
                        "%s" ILIKE
                  """ % (schema, table, search_column)
    query_text += """   %(search_text)s
                  """
    query_text += """ORDER BY
                        "%s"
                    LIMIT 1000
                  """ % search_column

    return query_text, query_dict
