# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 14:21:23 2016

@author: zbarge
"""
import os
import pytest
import pandas as pd
import listwise
from listwise.ListWise import ERROR_CODE

#Sample dataframe with a real email address and a fake one.
sample_emails = [['zekebarge@gmail.com'],['fakeemail']]
df = pd.DataFrame(sample_emails, columns=['email'], index=range(len(sample_emails)))
database_path = os.path.join(os.getcwd(), "listwise.db")
lw = listwise.ListWise(database_path, username="fake_username", api_key="fake_key", test_credentials=False)

#Sample response that would come from ListWise.
SAMPLE_RESPONSE = dict(email='zekebarge@gmail.com',email_status='clean',free_mail='yes',typo_fixed='no',dealno=0,clean_type=1)

def test_deep_clean():
    data = lw._deep_clean("zekebarge@gmail.com")
    assert data.get(ERROR_CODE, None) == 2, "Expected to get invalid credentials response."
    assert isinstance(data, dict), "Expected response to be a dictionary."
    
def test_bad_credentials():
    with pytest.raises(listwise.InvalidCredentialsError):
        lw.test_credentials()

def test_quick_clean():
    data = lw._quick_clean("zekebarge@gmail.com")
    assert data.get(ERROR_CODE, None) == 2, "Expected to get invalid credentials response."
    assert isinstance(data, dict), "Expected response to be a dictionary."

def test_parse_email_bad():
    BAD_EMAILS = ['bad/email.com','noemail@noemail', 'bad#email@gmail.com','bad$email@gmail.com']
    for e in BAD_EMAILS:
        assert lw.parse_email(e) == '', "Expected parse_email to remove bad email address {}".format(e)
        
def test_parse_email_good():
    GOOD_EMAILS = ['zekebarge@gmail.com','tony@yahoo.com','tina@gmail.com;tory@gmail.com']
    for e in GOOD_EMAILS:
        assert lw.parse_email(e) in e, "Expected parse_email to find a good email address in {}".format(e)

def test_insert_delete_response():
    email = SAMPLE_RESPONSE['email']
    lw.delete_email(email)
    lw.db.con.commit()
    
    lw._insert_response(SAMPLE_RESPONSE)
    lw.db.con.commit()
    
    data = lw.db.read_sql("SELECT * FROM EMAILS WHERE email = '{}'".format(email))
    assert not data.empty, "Expected an inserted record."
    
    lw.delete_email(email)
    lw.db.con.commit()
    
    data = lw.db.read_sql("SELECT * FROM EMAILS WHERE email = '{}'".format(email))
    assert data.empty, "Expected the delete method to remove the email record '{}'.".format(email)

def test_deep_clean_dataframe():
    tdf = lw.deep_clean_frame(df)
    assert isinstance(tdf, pd.DataFrame), "Expected to get a DataFrame back when deep_cleaning the dataframe."
    

if __name__ == "__main__":
    pytest.main()

    
    