import os
import sys
import json
import re


os.chdir('L:\\Auto_Opportunity_Analysis\\Json_Samples')


# ------------------------------------------
# check vnex dol greater than lx01 dos
# ------------------------------------------
cnt = 0
tot = 1

l = []

with open('0308_edi.json', 'r') as fr:
    for line in fr:

        if not re.match(r'\s{0,6}{', line):
            continue

        dicy = json.loads(line)
        tot += 1

        try:
            service_date = 'cust dos ' + dicy['service_date']
        except:
            service_date = ''

        if dicy['vx_date_of_loss'] <= dicy['LX01_service_date']:
            continue

        l.append('{}|{}|vx dol {} -> {} LX01 dos|{}'.format(dicy['cust_id'], dicy['pat_acct'], dicy['vx_date_of_loss'], dicy['LX01_service_date'], service_date))
        cnt += 1

[print(rec) for rec in sorted(l)]
print('{} / {}'.format(cnt, tot))


# ------------------------------------------
# generate new json
# ------------------------------------------
tot = 0

with open('0308_edi.json', 'r') as fr, open('0308_edi1.json', 'w') as fw0:
    for line in fr:

        if not re.match(r'\s{0,6}{', line):
            continue

        dicy = json.loads(line)

        if dicy['vx_date_of_loss'] <= dicy['LX01_service_date']:
            print(line, end='', file=fw0)
            tot += 1

print(tot)

