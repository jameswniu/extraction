import os
import glob
import sys
import json

import pandas as pd
import psycopg2.extras

from config import user_db, passwd_db


os.chdir('L:\\architecture\\__status_codes_customer_codes\\_development_configs')


#----
# convert excel to csv or txt
#----
file_list = []
[file_list.append(file) for file in glob.glob('tpl_*.xlsx')]

for file in file_list:
    print('working...', file)
    read_file = pd.read_excel(file)
    read_file.to_csv(file.replace('xlsx', 'txt'), sep='|', index=None, header=True)


#----
# extract records from database and csv and compare differences
#----
new_sql = """
select 
    cust_id::text, 
    state, 
    cust_payer_id, 
    cust_payer_name, 
    cust_financial_class, 
    return_days_edos::text, 
    return_days_placement::text, 
    priority_order::text, 
    return_all_accounts_ind
from 
    tpl_cust_return_accounts_config;
"""
params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': 'user_db',
    'password': 'passwd_db'
}
con = psycopg2.connect(**params)
cur = con.cursor(cursor_factory = psycopg2.extras.DictCursor)
cur.execute(new_sql)

db = [row for row in cur] # new accts from yest
print(db[:3], db[-2:])

uq = []
with open('tpl_cust_return_accounts_config_v16.txt', 'r') as fr:
    next(fr)
    for line in fr:
        tmp = line.rstrip().split('|')
        uq.append(tmp)
print(uq[:3], uq[-2:])

res = set(uq).symmetric_difference(set(db)) #take non-intersections
print(); print(res)

