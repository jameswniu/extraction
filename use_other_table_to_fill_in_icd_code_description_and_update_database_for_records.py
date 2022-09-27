#! /usr/bin/python3
import os
import sys
import re

import psycopg2
import psycopg2.extras

from utils.rev_email import send_mail
from utils.db_conn import DBHandler


#----
# To insert DX code descriptions for 837P records
#----

conn = DBHandler().get_session()
cur = conn.cursor()


sql_select = """
           select pm_sk, content->>'diagnoses' as codes from tpl_pre_billing_records
           where pm_sk > 2200000 and cust_id not in (9999, 9990, 710, 742, 739, 745) and status in ('PB' ,'W')
                 and content->>'claim_type' in ('837P')
                 and content->'dx_code_desc' is null limit 2000
           """
sql_get_desc = """
            select string_agg( code || '|' || info, '@') from tpl_icd_codes
            where code = ANY( regexp_split_to_array('{}', ','));
            """

sql_dx_code_desc_update = """
           update tpl_pre_billing_records set content = content || jsonb_build_object(
            'dx_code_desc',  '{}') where pm_sk  = {} """


print(' updating dx description  ...' )

no_dx_info = []

count = 0
records = {}
cur.execute(sql_select)

for line in cur:
    records[line.pm_sk] = line.codes.replace('.', '').replace(' ', '')
    count += 1

updated = 0
if count > 0 :
    for k, codes in records.items() :
        sql = sql_get_desc.format( codes )
        cur.execute(sql)
        x = cur.fetchone()
        #print(k, codes, x) # keep for debug
        if x is None or x[0] is None: # if no ICD description
            no_dx_info.append("{}: {}".format(k, codes))
            continue
        details = x[0].replace("'", "")
        details = details.upper()
        upd_sql = sql_dx_code_desc_update.format(details, k)
        #print(upd_sql)
        cur.execute(upd_sql)
        updated += 1

    conn.commit()


print(' updated {} records dx_code_desc !'.format(updated), file=sys.stderr)


if no_dx_info:
    msg = '\r\n'.join(no_dx_info)
    print(msg, file=sys.stderr)
    send_mail("New ICD Code", msg, 'yi.yan@medlytix.com')

