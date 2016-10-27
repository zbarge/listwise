# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 11:17:23 2016

@author: zbarge
"""
import sqlite3 
import pandas as pd

class SimpleSQLite3:
    """ 
    A simplified sqlite3 class with a connection & cursor.
    Interacts with Pandas.
    """
    def __init__(self, database_path):
        self._database_path = database_path
        self._con = sqlite3.connect(self._database_path)
        self._cur = self._con.cursor()
    
    @property
    def con(self):
        """sqlite3.connection """
        return self._con
        
    @property
    def cur(self):
        """sqlite3.connection.cursor() object. """
        return self._cur
        
    @property
    def Row(self):
        return sqlite3.Row
    
    def _connect(self, dbpath):
        self._con = sqlite3.connect(dbpath)
        self.DBPath = dbpath
        self._cur = self.con.cursor()

    def set_row_factory(self, row_factory):
        self.con.row_factory = row_factory
        self._cur = self.con.cursor()

    def show_tables(self):
        show_tables_query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"  
        return self.liteQuery(show_tables_query)
        
    def count_records(self, tablename):
        self.cur.execute("SELECT COUNT(*) FROM {}".format(tablename))
        return self.cur.fetchone()[0]
        
    def add_column(self,table,column,dtype):
        sql = "ALTER TABLE {} ADD COLUMN {} {}".format(table,column,dtype)
        with self.con:
            self.cur.execute(sql)
            
    def drop_table(self, table_name):
        self.cur.execute("DROP TABLE {}".format(table_name))

    def get_columns(self, table):
        df = self.read_sql("SELECT * FROM {} LIMIT 1".format(table))
        return df.columns.tolist()
        
    def count_columns(self, table):
        return len(self.get_columns(table))
        
    def sql_exists(self, table_name, fields=None):
        """
        Makes a small query testing if the
        table exists in the database
        or if the fields exist in the table.
        Returns True if no errors, False otherwise.
        """
        assert not fields or isinstance(fields, list), "fields must be a list or None, not {}".format(type(fields))
        try:
            df = self.read_sql("SELECT * FROM {} LIMIT 1".format(table_name))
            if fields:
                df = df.loc[:, fields]
            return True
        except Exception:
            return False
            
    def get_table(self, table, **kwargs):
        return self.read_sql("SELECT * FROM {}".format(table), **kwargs)
        
    def read_sql(self, sql, **kwargs):
        """pandas.read_sql(sql, con, index_col=None, 
            coerce_float=True, params=None, parse_dates=None, 
            columns=None, chunksize=None) """
        return pd.read_sql(sql, self.con, **kwargs)

    def __del__(self):
        try:
            self.con.close()
        finally:
            del self


