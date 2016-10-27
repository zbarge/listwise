# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 11:31:36 2016

@author: zbarge
"""
import os
import sqlite3
import requests
import pandas as pd
from time import sleep
from .SimpleSQLite3 import SimpleSQLite3
            
#======================================================#
"""These lists provide information to the ListWise.parse_email method. """

# Enter domains to exclude from being considered valid
ILLEGAL_EMAIL_DOMAINS = []

ILLEGAL_EMAIL_CHARS = ["'", "/", "#", "\"", 
                       "\\", "$", "%", "&", 
                       ")", "(", "*"]
EMAIL_CHARS_TO_SPLIT = [';', ' ', ',', ':', '|']
ILLEGAL_SCRUB_ITEMS = ILLEGAL_EMAIL_DOMAINS + ILLEGAL_EMAIL_CHARS
#=======================================================#

EMAILS_SQL_TABLE = """CREATE TABLE emails (
    email        VARCHAR (30) UNIQUE ON CONFLICT REPLACE,
    email_status VARCHAR (30),
    error_code   VARCHAR (30),
    error_msg    VARCHAR (30),
    free_mail    VARCHAR (30),
    insertdate   DATETIME     DEFAULT (DATETIME('now', 'localtime') ),
    typo_fixed   VARCHAR (30),
    dealno       INT (30)     DEFAULT (0),
    clean_type   INT (30)     DEFAULT (0),
    email_id     INTEGER      PRIMARY KEY ON CONFLICT REPLACE AUTOINCREMENT,
    updatedate   DATETIME     DEFAULT (DATETIME('now', 'localtime') ) 
);"""

DOMAINS_SQL_TABLE = """CREATE TABLE domains (
    domain_id INTEGER PRIMARY KEY ON CONFLICT REPLACE AUTOINCREMENT,
    domain VARCHAR(30) UNIQUE ON CONFLICT IGNORE, 
    valid INT(1))"""
              
TABLE_STRUCTURES = {'emails':EMAILS_SQL_TABLE, 'domains': DOMAINS_SQL_TABLE}

# Table names are emails, field names are email
email, emails, email2, emails2 = 'email', 'emails', 'email2', 'emails2'
EMAIL = 'email'
EMAILS = 'emails'
EMAIL2 = 'email2'
EMAILS2 = 'emails2'
DOMAIN = 'domain'
DOMAINS = 'domains'

#The email_status field from ListWise can contain any of the following statuses
#The only statuses considered valid are "clean", "catch-all"
#processing status data need to be rerun.

EMAIL_STATUS = 'email_status'
CLEAN = 'clean'
CATCHALL = 'catch-all'
PROCESSING =  'processing'
BADMX = 'bad-mx'
BOUNCED = 'bounced'
INVALID = 'invalid'
NOREPLY = 'no-reply'
SPAMTRAP = 'spam-trap'
SUSPICIOUS = 'suspicious'
UNKNOWN = 'unknown'


FREE_MAIL = 'free_mail'
TYPO_FIXED = 'typo_fixed'
ERROR_CODE = 'error_code'

class InvalidCredentialsError(Exception): pass
    
class ListWise:
    """ 
    A python class wrapping the API to ListWise e-mail address cleaner. 
    https://www.listwisehq.com/email-address-cleaner/index.php
    This class allows the user to validate an individual 
    e-mail addresses or a Pandas DataFrame. 
    The responses from ListWise are stored in a 
    database and valid e-mails are returned to the DataFrame.
    Invalid e-mails are replaced with nothing.
    This process seems to take about 0.75 seconds per e-mail address.     
    """
    def __init__(self, database_path, username=None, api_key=None, test_credentials=True):
        self._api_key = api_key
        self._username = username
        self._db_path = database_path
        self._queued_emails = []
        self._bad_emails = []
        self._errored_responses = {}
        self._db = SimpleSQLite3(self._db_path)
        self._db.set_row_factory(sqlite3.Row)
        self._create_tables()
        if test_credentials:
            self.test_credentials()
        
    @property
    def db(self):
        """The connection to PandaLite/SQLite database. """
        return self._db
        
    def test_credentials(self):
        """Checks a ListWise API response and 
        raises an InvalidCredentialsError if the credentials
        are invalid. Returns True otherwise."""
        data = self._deep_clean('zekebarge@gmail.com')
        error = data.get(ERROR_CODE, None)
        if error in (1,2):
            raise InvalidCredentialsError("Credentials are invalid for user '{}'".format(self._username))
        return True
        
    def _create_tables(self):
        for table,contents in TABLE_STRUCTURES.items():
            if not self.db.sql_exists(table):
                with self.db.con:
                    self.db.cur.execute(contents)
            
    def _insert_response(self, r, dealno=0, clean_type=0, table=EMAILS):
        """ 
        r: response dictionary from self._quick_clean or self._deep_clean
        dealno: default (0), the deal number of the deal for the data being processed.
        clean_type: 0 = quick_clean, 1 = deep_clean
        """    
        sql = """INSERT INTO {} (email,email_status,free_mail,typo_fixed,dealno,clean_type) 
                VALUES ('{}','{}','{}','{}',{},{});""".format(
                table, r[EMAIL], r[EMAIL_STATUS], r[FREE_MAIL], r[TYPO_FIXED], dealno, clean_type)
        self.db.cur.execute(sql)
        
    def _parse_valid_response(self, email, resp):
        try:
            if resp[EMAIL_STATUS] in [CLEAN, CATCHALL]:
                return resp[EMAIL]
            elif resp[EMAIL_STATUS] == PROCESSING:
                return resp[EMAIL]
            else:
                return ''
        except (KeyError,TypeError):
            return ''

    def get_domain(self, email):
        """
        Either returns all characters after '@' 
        or returns None if no '@' exists. 
        """
        try:
            return str(email).split('r@')[1]
        except:
            return None
            
    def parse_email(self, email):
        """
        Validates the basic components of an
        email address with a few tests. 
        Returns the lowercased email address if tests pass.
        Returns an empty string if a test fails.
        """
        if not email:
            return ''
            
        email = str(email).lower().replace('.comhome','.com')
        
        for item in EMAIL_CHARS_TO_SPLIT:
            if item in email:
                email = email.split(item)[0]
                
        for item in ILLEGAL_SCRUB_ITEMS:
            if item in email:
                return ''
                
        if not "@" in email:
            return ''
        elif not "." in email:
            return ''
        elif not len(email) > 5:
            return ''
            
        return email
        
    def pre_process_frame(self, df, col=None):
        """Runs class method parse_email, 
        drops duplicates, 
        and then drops records with no email address. """
        col = (EMAIL if not col else col)
        df.loc[:,col] = df.loc[:,col].apply(self.parse_email)
        df.drop_duplicates([col],inplace=True)
        return self.drop_missing_emails(df,col=col)

    def _quick_clean(self, email):
        url = "https://api.listwisehq.com/clean/quick.php?email={}&api_key={}".format(email, self._api_key)
        return requests.get(url).json()
        
    def _deep_clean(self, email):
        url = "https://api.listwisehq.com/clean/deep.php?email={}&api_key={}".format(email, self._api_key)
        return requests.get(url).json()
        
    def delete_email(self, email):
        """Deletes an email address from the emails table.
        You must commit/rollback the transaction on your own."""
        sql = "DELETE FROM emails WHERE email = '{}'".format(
                email)
        self.db.cur.execute(sql)
        
    def check_db(self, email, clean_type=1):
        """
        Checks the database for a matching clean/catchall status email address.
        If one is found, it is returned as {'email': 'email@example.com'}
        Returns None if no match was found.
        """
        try:
            sql = """
                  SELECT * FROM emails WHERE email = '{}' 
                  AND clean_type = {}
                  AND email_status IN('clean','catch-all')
                  LIMIT 1
                  """.format(email, clean_type)
            self.db.cur.execute(sql)
            resp = self.db.cur.fetchone()
            if resp:
                return {EMAIL:resp[EMAIL]}
        except:
            print("sql error: {}".format(sql))
            return None
            
    def db_clean_one(self, email, clean_type=1):
        """Cleans an email address by checking the local database (and thats it) 
        returning None if no match exists. """
        res = self.check_db(email,clean_type=clean_type)
        if res:
            return res[EMAIL]
            
    def quick_clean_one(self, email, dealno=0):
        
        if not pd.notnull(email) or not email:
            return email
            
        resp = self._quick_clean(email)
        #print("{}: {}".format(resp['email'],resp['email_status']))
        error = resp.get(ERROR_CODE, None)
        if error:
            self._errored_responses.update({email:resp})
            return email
            
        self._insert_response(resp,dealno=dealno,clean_type=0)
        
        return self._parse_valid_response(email,resp)
            
    def quick_clean_one2(self, email, dealno=0):
        resp = self.check_db(email, clean_type=0)
        try:
            return resp[EMAIL]
        except:
            return self.quick_clean_one(email,dealno=dealno)
        
    def quick_clean_frame(self, df, email_col=None, clean_col='EMAIL_CLEANED', dealno=0):
        email_col = (EMAIL if not email_col else email_col)
        clean_col = (email_col if not clean_col else clean_col)

        df.loc[:,clean_col] = df.loc[:,email_col].apply(self.parse_email).apply(self.quick_clean_one2,args=([dealno]))
        self.db.con.commit()
        return df
        
    def deep_clean_one(self, email, dealno=0):
        """ 
        Checks the email against the deep clean API and inserts the response into the database.
        Note: The database must be committed after running this to make changes stick.
        """
        if not pd.notnull(email) or not email:
            return email
        resp = self._deep_clean(email)
        #print("{}: {}".format(resp['email'],resp['email_status']))
        error = resp.get(ERROR_CODE,None)
        
        if error:
            self._errored_responses.update({email:resp})
            return email
            
        self._insert_response(resp, dealno=dealno, clean_type=1)
        
        return self._parse_valid_response(email, resp)
        
    def deep_clean_one2(self, email, dealno=0):
        """ 
        Checks the email address against the database and tries to return a result.
        If no result, reruns the email against the API. 
        Note: The database must be committed after running this to make changes stick.
        """
        resp = self.check_db(email, clean_type=1)
        try:
            return resp[EMAIL]
        except:
            return self.deep_clean_one(email, dealno=dealno)
            
    def deep_clean_frame(self, df, email_col=None, clean_col='EMAIL_CLEANED', dealno=0):
        """
        Cleans a pandas.DataFrame using the deep_clean_one2 class method. 
        Cleaned emails are stored in the database for future use.
        
        PARAMETERS:
        ============
        df - pandas.DataFrame object with a column of email addresses
        
        email_col - (string) of the name of the 
            column containing email addresses
        
        clean_col - (string) of the name of the column to store 
            clean email addresses, or None to replace the email_col
            
        dealno - (int) Defaults to 0, optionally store a deal number 
            with each email address cleaned.
        """
        email_col = (EMAIL if not email_col else email_col)
        if not clean_col:
            clean_col = email_col

        df.loc[:,clean_col] = df.loc[:,email_col].apply(self.parse_email).apply(self.deep_clean_one2,args=([dealno]))
        self.db.con.commit()
        return df
        
    def deep_processing_rerun_all(self):
        """ 
        Pulls records that are still in the processing status and reruns them against 
        the deep clean API. New responses are passed back into the database.
        """
        sql = """SELECT * FROM emails 
                WHERE email_status = 'processing' 
                AND clean_type = 1"""
        df = self.db.read_sql(sql)

        for i in range(df.index.size):
            rec = df.loc[i, :]
            self.deep_clean_one(rec[EMAIL], dealno=rec['dealno'])
        self.db.con.commit()
        print('Reprocessed {} records that were stuck in the processing status'.format(df.index.size))
        
    def deep_processing_rerun(self, dealno=0, thresh=0.05, max_tries=5):
        """Reprocesses records that are in 'processing' up to max_tries times 
        or until the count of unprocessed records is less than thresh. """
        
        assert thresh < 1 and thresh > 0, "The threshold parameter should be a decimal less than 1 and greater than 0."
        self.db.cur.execute("SELECT count(*) as count FROM emails WHERE dealno = {}".format(dealno))
        count = self.db.cur.fetchone()['count']
        thresh = thresh * count
        sql = """SELECT * FROM emails 
                WHERE dealno = {} 
                AND email_status = 'processing'
              """.format(dealno)
        df_processing = self.db.read_sql(sql)
        
        tries = 0
        while df_processing.index.size > 0:
            tries += 1
            ct = df_processing.index.size
            if tries > max_tries:
                raise Exception("Tried to reprocess {}x with no luck...giving up.".format(max_tries))
            if tries > 1:
                print("Waiting 2 minutes before attempt #{} of reprocessing.".format(tries))
                sleep(60 * 2)
            if tries > 2 and ct < thresh:
                break
                
            if ct > 0:
                print("Reprocessing {} records for deal {}".format(ct,dealno))
            else:
                return print("Found no records to reprocess")
            df_processing = self.db.read_sql(sql)
            self.deep_clean_frame(df_processing, email_col=EMAIL,clean_col=None,dealno=dealno)
        print("Deep processing rerun completed successfully on deal {}".format(dealno))
            
    def merge_email_frame(self, df, col=None, sql=None):
        """Merges a pandas.DataFrame with clean emails matching from the database. 
        As this database grows, this operation will take longer to select & join... 
        we need to just suppress the bad ones."""
        sql = ("SELECT email,email_status from emails WHERE email_status IN('clean','catch-all')" 
                if not sql else sql)
        col = (EMAIL if not col else col)
        clean_df = self.db.read_sql(sql)
        clean_df.rename(columns={EMAIL:col}, inplace=True)

        df.loc[:,col] = df.loc[:,col].apply(self.parse_email)
        df = df.dropna(subset=[col])
        df = pd.merge(df, clean_df, how='inner', left_on=col, right_on=col)
        return df[df[col] != '']
        
    def suppress_email_frame(self, df, col='EMAIL', clean_type=1):
        """The opposite of deep_email_merge class method.
        Bad emails are pulled from the database and suppressed from the original data."""
        assert clean_type in (0,1), "Invalid clean_type {}, must be 0 or 1."
        sql = """
            SELECT email,email_status from emails 
            WHERE email_id NOT IN
                (SELECT email_id 
                FROM emails 
                WHERE email_status IN('clean','catch-all'))
            AND clean_type = {}
            """.format(clean_type)
        bad_df = self.db.read_sql(sql)
        bad_df.rename(columns={EMAIL:col},inplace=True)

        df.loc[:,col] = df.loc[:,col].apply(self.parse_email)
        df = df.dropna(subset=[col])
        
        df2 = pd.merge(df, bad_df, how='left', left_on=col, right_on=col)
        statuses = [x for x in df2.loc[:,'email_status'].unique() if pd.notnull(x)]
        df2 = df2[~df2.email_status.isin(statuses)]
        
        return self.drop_missing_emails(df2,col=col)
        
    def drop_missing_emails(self, df, col=None):
        """
        drops records with nan values and empty strings.
        df (pandas.Series/pandas.DataFrame) with a 
            column containing email addresses
            
        col (string) - the name of the column containing emails 
            (only necessary if df is a pandas.DataFrame)
            
        returns - a pandas.DataFrame or pandas.Series with blank records removed.
        """
        col = (EMAIL if not col else col)
        if isinstance(df,pd.Series):
            df = df.dropna()
            return df[df != '']
        else:
            df = df.dropna(subset=[col])
            return df[df[col] != '']
            
    def count_matching_emails(self, df, col=None,verify_integrity=True,thresh=0.05):
        """
        Counts emails in a dataframe that exist in the listwise database.
        
        PARAMETERS:
        ================
        df - (pandas.DataFrame) with an email column to check against the database
        col - (str) the label of the column with emails, defaults to EMAIL
        
        verify_integrity: (bool) - default True checks the original 
            count of parseable emails against the count in the database.
            
        thresh: (float) - if verify_integrity is True, the thresh is 
            the max percentage of missing emails allowed to not raise an error.
        """
        col = (EMAIL if not col else col)
        TABLE = "rdsupp_temp"
        idx = "rdsupp_index"
        df.loc[:,col] = df.loc[:,col].apply(self.parse_email)
        df = self.drop_missing_emails(df,col=col)
        df_check = self.drop_missing_emails(df.loc[:,col],col=col)
        df_check.to_sql(TABLE,self.db.con,index_label=idx,if_exists='replace')
        sql = "SELECT COUNT(*) as count FROM {t1} WHERE {col} IN (SELECT email FROM emails)".format(t1=TABLE,col=col)
        self.db.cur.execute(sql)
        count_matching = self.db.cur.fetchone()['count']
        if verify_integrity:
            assert thresh < 1 and thresh > 0, "The thresh parameter should be a decimal less than 1 and greater than 0."
            count_orig = df.index.size
            thresh = round(thresh * count_orig,0)
            missing = count_orig - count_matching
            if missing > 0:
                if missing > thresh:
                    raise Exception("Missing {} records from the database. Threshold is {}".format(missing,thresh))
                else:
                    print("Missing {} records from the database - no error since the threshold is {}".format(missing,thresh))
        return count_matching
        
    def process_domains(self, save_path=None):
        """Gathers unique domain names from the database.
        imports new domain names to the domains table."""
        emails = self.db.read_sql("SELECT * FROM emails")
        emails.loc[:, email2] = emails.loc[:, email].apply(self.parse_email)            
        emails.loc[:, DOMAIN] = emails.loc[:, email2].apply(self.get_domain)
        emails.drop_duplicates([DOMAIN], inplace=True)
        if save_path:
            emails.to_csv(save_path, index=False)
        emails.loc[:,DOMAIN].to_sql(DOMAINS, self.db.con, if_exists='append', index=False)
        
    def _reparse_database_emails(self):
        """ 
        Run this after updating ListWised.parse_emails 
        method to reparse the emails in the database 
        and rerun changed emails through ListWised.
        Deletes the old email addresses from the database.
        """
        emails = self.db.read_sql("SELECT * FROM emails")
        emails.loc[:, email2] = emails.loc[:, email].apply(self.parse_email)
        diff_emails = emails.loc[emails[email2] != emails[email], [email, email2, 'dealno']]
        
        if not diff_emails.empty:
            diff_emails.dropna(how='any', axis=0, inplace=True)
            diff_emails.reset_index(drop=True, inplace=True)
            diff_emails = diff_emails.loc[diff_emails[email2] != '', :]
            print("Reprocessing {} records that changed when reparsed.".format(diff_emails.index.size))
            try:
                for i in range(diff_emails.index.size):
                    #Reprocess the re-parsed email with the original dealno.
                    r = diff_emails.iloc[i]
                    self.deep_clean_one(r[email2], r['dealno'])
                    self.delete_email(r[email])
                self.db.con.commit()
                self.deep_processing_rerun_all()
                print("Re-parse processing complete.")
            except:
                self.db.con.rollback()
                raise
        else:
            print("No new database email records found to re-process.")       

    def process_multiple_files(self, filepaths, email_col='EMAIL',min_size=100, threshold=0.05):
        """
        Processes multiple filepaths
        runs the email addresses against the ListWise API - deep clean.
        Saves updated files, appending names with "-LISTWISED"
        uploads cleaned up files to DropBox.
        Notifies art at the end with all new filenames
        
        PARAMETERS:
        ========================
        lw: A ListWise object
        filepaths: (list) - filepaths to process.
        email_col: (string) - the column in each file that contains the email addresses to process.
        min_size: (int) - the minimum # of email addresses that must exist in the file in order to process it.
        threshold: (float) - a float representing a min percentage of processed records to gather before exporting the data.
        
        Returns (list) containing the new filepaths of the processed files.
        """
        new_paths = []
        for f in filepaths:
            df = pd.read_csv(f)
            df = self.pre_process_frame(df, col=email_col)
            orig_size = df.index.size
            FLAG = True
            if orig_size < min_size:
                pass
            else:
                print("Cleaning {}".format(f))
                
                try: # Try to first do an easy match with results directly from the database. (Very fast compared to API calls)
                    self.count_matching_emails(df, col=email_col, verify_integrity=True, thresh=threshold)
                except Exception as e:
                    
                    print("{}\n Calling missing emails from remote server.".format(e))
                    df = self.deep_clean_frame(df,dealno=0,clean_col=email_col) # The long way - calling the API.
                    
                    try:
                        self.deep_processing_rerun(dealno=0,thresh=0.05,max_tries=5) # Handling records stuck in processing.
                        count = self.count_matching_emails(df, col=email_col, verify_integrity=True, thresh=threshold)
                        print("Successfully matched {} records".format(count))
                    except Exception as e:
                        
                        FLAG = False # Stop this from finalizing...too many records stuck in processing/not in database...somethings wrong.
                        print("Failed to reprocess some records for {}\n Error: {}".format(f,e))
                        
                if FLAG:
                    df = self.suppress_email_frame(df, col=email_col, clean_type=1)
                    new_path = os.path.splitext(f) + "ListWised.csv"
                    df.to_csv(new_path, index=False)
                    new_paths.append(new_path)
                    
        self.deep_processing_rerun_all() # Wraps up making one last try at rerunning any emails stuck in processing (for next time).
        return new_paths
    
def test_merge_vs_suppress_email_frame(lw, filepath):
    opath = os.path.splitext(filepath)[0] + '-LWMERGED.csv'
    opath2 = os.path.splitext(filepath)[0] + 'LWSUPPRESSED.csv'
    df = pd.read_csv(filepath)
    o = df.index.size
    data1 = lw.merge_email_frame(df)
    data1 = lw._format_email_frame(data1)
    d = data1.index.size
    print("DATA 1: Previous: {} Post: {}".format(o,d))
    data1.to_csv(opath)
    
    data2 = lw.suppress_email_frame(df)
    data2 = lw._format_email_frame(data2)
    d = data2.index.size
    print("DATA 2: Previous: {} Post: {}".format(o,d))
    data2.to_csv(opath2)

if __name__ == '__main__':
    pass


    


    

    
    


    
    
    