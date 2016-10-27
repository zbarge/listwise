ListWise - Python REST API Wrapper for ListWise email validation.
=================================================================

In order to use all of the features of this package you must have an active subscription with listwisehq.
https://www.listwisehq.com/email-address-cleaner/index.php

This packaged was developed during my employment with Kennedy Marketing Group.
Thanks to them for giving me permission to open-source this.

listwise enables users to easily clean email addresses via the ListWise REST API.

Connecting to the API
======================

listw = listwise.ListWise("C:/listwise_data.db", username, api_key)


One-off email validation
=========================
email = 'zekebarge@gmail.com'

deep_cleaned = listw.deep_clean_one(email)

quick_cleaned = listw.quick_clean_one(email)

listw.db.con.commit() #To commit your updates to the database



Bulk e-mail validation using Pandas
====================================
import pandas as pd

df = pd.read_csv("C:/path/to/file.csv")

#All updates are automatically committed to SQLite.

deep_cleaned_df = listw.deep_clean_frame(df)

quick_cleaned_df = listw.quick_clean_frame(df)






