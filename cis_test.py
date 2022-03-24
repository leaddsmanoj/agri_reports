#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sqlalchemy
from datetime import datetime
import time
# Python code to illustrate and create a
# table in database
import mysql.connector as mysql


# In[2]:


engine_read = sqlalchemy.create_engine('mysql+pymysql://report:fa4SsPAwfDAKw4Nd@database-read-1.c2c32urb2pzx.ap-south-1.rds.amazonaws.com:3306/agribazaar')
engine_read_govt = sqlalchemy.create_engine('mysql+pymysql://manoj:cJtDsZ4UF6djRRXg@server12.agribazaar.com:3307/Stardb')
engine_write = sqlalchemy.create_engine('mysql+pymysql://manoj:cJtDsZ4UF6djRRXg@agri-reports.c2c32urb2pzx.ap-south-1.rds.amazonaws.com:3306/agri_reports')


# In[14]:


df_customer = pd.read_sql('st_users',engine_read_govt)
crf_master = pd.read_sql('crf_master',engine_read_govt)
crf_client_type = pd.read_sql('crf_client_type',engine_read_govt)
df18 = pd.read_sql('customers',engine_read)


# In[15]:


start_time = datetime.now()
print('Loading table... st_users')
df_customer.to_sql('st_users',engine_write,index=False,if_exists='replace', schema='govt_db')
time_to_load = (datetime.now() - start_time).seconds
print(time_to_load,'Sec\nTransfer completed !!')

start_time = datetime.now()
print('Loading table... crf_master')
crf_master.to_sql('crf_master',engine_write,index=False,if_exists='replace', schema='govt_db')
time_to_load = (datetime.now() - start_time).seconds
print(time_to_load,'Sec\n')

start_time = datetime.now()
print('Loading table... customers')
df18.to_sql('customers',engine_write,index=False,if_exists='replace', schema='private_db')
time_to_load = (datetime.now() - start_time).seconds
print(time_to_load,'Sec\n')


start_time = datetime.now()
print('Loading table... crf_client_type')
crf_client_type.to_sql('crf_client_type',engine_write,index=False,if_exists='replace', schema='govt_db')
time_to_load = (datetime.now() - start_time).seconds
print(time_to_load,'Sec\n')


# In[ ]:


# Open database connection
db = mysql.connect(host="agri-reports.c2c32urb2pzx.ap-south-1.rds.amazonaws.com",user="manoj",password="cJtDsZ4UF6djRRXg",database="agri_reports")
cursor = db.cursor()


# In[ ]:


drop_customer_not_exists_in_pdb = """drop table if exists agri_reports.customer_not_exists_in_pdb;"""
create_customer_not_exists_in_pdb = """create table agri_reports.customer_not_exists_in_pdb as select * from govt_db.st_users su where not EXISTS (select 1 from private_db.customers c WHERE su.client_id = c.gov_db_client_id);"""
update_customer_not_exists_in_pdb = """UPDATE agri_reports.customer_not_exists_in_pdb SET client_type = CASE WHEN client_type = '11' THEN 'Farmer' else 'Trader' END;"""
drop_st_users_complete_match = "DROP table if exists agri_reports.st_users_complete_match;"

create_st_users_complete_match = """create table agri_reports.st_users_complete_match as 
select * from govt_db.st_users su where EXISTS  
(select 1 from private_db.customers c WHERE su.client_id = c.gov_db_client_id);"""

update_st_users_complete_match = """UPDATE agri_reports.st_users_complete_match
SET client_type = 
    CASE 
    WHEN client_type = '11' THEN 'Farmer' else 'Trader'
    END;"""

drop_st_users_merge = "drop table if exists agri_reports.st_users_merge;"

create_st_users_merge = """create table agri_reports.st_users_merge as 
select * from agri_reports.st_users_complete_match
Union all
select * from agri_reports.customer_not_exists_in_pdb;"""

drop_st_users_final = """drop table if exists agri_reports.st_users_final;"""

create_st_users_final = """create table agri_reports.st_users_final as
select a.*, c.customer_type from agri_reports.st_users_merge a 
left join private_db.customers c on a.client_id = c.gov_db_client_id;"""


# In[ ]:


cursor.execute(drop_customer_not_exists_in_pdb)
print('drop_customer_not_exists_in_pdb')
cursor.execute(create_customer_not_exists_in_pdb)
print('create_customer_not_exists_in_pdb')
cursor.execute(update_customer_not_exists_in_pdb)
print('update_customer_not_exists_in_pdb')
cursor.execute(drop_st_users_complete_match)
print('drop_st_users_complete_match')
cursor.execute(create_st_users_complete_match)
print('create_st_users_complete_match')
cursor.execute(update_st_users_complete_match)
print('update_st_users_complete_match')
cursor.execute(drop_st_users_merge)
print('drop_st_users_merge')
cursor.execute(create_st_users_merge)
print('create_st_users_merge')
cursor.execute(drop_st_users_final)
print('drop_st_users_final')
cursor.execute(create_st_users_final)
print('create_st_users_final')
print('Done....')


# In[ ]:


# st_users_exp drop table
drop_st_users_exp = """drop table if exists agri_reports.st_users_exp;"""

# create st_users_exp
create_st_users_exp = """create table agri_reports.st_users_exp as 
select a.*, c.customer_type from govt_db.st_users a 
left join private_db.customers c on a.client_id = c.gov_db_client_id;"""

# update st_users_exp
update_st_users_exp = """UPDATE agri_reports.st_users_exp SET client_type = CASE WHEN client_type = '11' THEN 'Farmer' else 'Trader' END;"""


# In[ ]:


print('drop_st_users_exp')
cursor.execute(drop_st_users_exp)
print('create_st_users_exp')
cursor.execute(create_st_users_exp)
print('update_st_users_exp')
cursor.execute(update_st_users_exp)
print('Done!!')


# In[ ]:


cursor.close()


# In[ ]:


print('Script Run Successful!!')

