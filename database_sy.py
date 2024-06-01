#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
import sqlite3
import pandas as pd
import numpy as np


# In[2]:


# 데이터베이스에 테이블생성
def create_table(data_name, data_table_name):
    # Create database
    con = sqlite3.connect('./my_db.db')
    cursor = con.cursor()
    
    # Drop the table if table was Existing.
    table_list = [data_name]

    for i in table_list:
        cursor.execute(f"DROP TABLE IF EXISTS {i}")
        print(f"\n\n Existing table '{i}' dropped, and new table is created")
    
    data = pd.read_csv(f"./{data_name}.csv")
    data.to_sql(f'''{data_table_name}''', con = con, index=False)

    con.commit()
    con.close()

# 데이터베이스에 생선된 테이블 확인    
def making_dataframe(table_name):
    con = sqlite3.connect('./my_db.db')
    cursor = con.cursor()

    cursor.execute(f"SELECT * from {table_name}")
    cols = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(data = cursor.fetchall(), columns=cols)
    
    con.commit()
    con.close()
    return df

# 새롭게 생성된 데이터프래임을 데이터베이스에 저장
def create_new_table(data_frame, data_table_name):
    # Create database connection
    con = sqlite3.connect('./my_db.db')
    cursor = con.cursor()
    
    # Drop the table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {data_table_name}")
    print(f"\n\nExisting table '{data_table_name}' dropped, and new table is created")
    
    # Create a new table and insert data from the DataFrame
    df = data_frame
    df.to_sql(data_table_name, con, index=False, if_exists='replace')
    
    # Commit the transaction and close the connection
    con.commit()
    con.close()


# In[ ]:




