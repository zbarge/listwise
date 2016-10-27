# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 13:27:01 2016

@author: zbarge
"""

import os
import listwise
import pandas as pd

USERNAME = "" #ENTER USERNAME
API_KEY = "" #ENTER API_KEY
TEST_CREDENTIALS = False # Set to False if you dont have valid credentials






#Constructs a sample dataframe with a real email address and a fake one.
sample_emails = [['zekebarge@gmail.com'],['fakeemail']]
df = pd.DataFrame(sample_emails, columns=['email'], index=range(len(sample_emails)))


database_path = os.path.join(os.getcwd(), "listwise.db")

# Initializes the ListWise object.
lw = listwise.ListWise(database_path, username=USERNAME, api_key=API_KEY, test_credentials=TEST_CREDENTIALS)

# Applies the listwise.parse_email method to the email column which drops the fakeemail record.
df.loc[:,'email'] = df.loc[:,'email'].apply(lw.parse_email)

# Applies the listwise.quick_clean_one method to the email column which passes the good email address to ListWise.
df.loc[:,'email'] = df.loc[:,'email'].apply(lw.quick_clean_one)


# Get a dataframe of the data in the emails table.

data = lw.db.read_sql("SELECT * FROM emails")
print(data)

print(df)

