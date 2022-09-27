#!/usr/bin/env python3
import json
import sys
import re


def check_sum(dicy):
    sum = 0.0
    for ky in dicy:
        if 'LX' in ky and 'charge' in ky and 'non_covered' not in ky:
            sum += float(dicy[ky])
    sum = round(sum, 2)
    return sum


print('{}|{} --- {}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}-{} --- {}'.format(
    'pending_info', 'tax_id', 'inst_prof', 'cust_id', 'pat_acct'
    , 'claim_num', 'pm_sk', 'insurance_name'
    , 'pat_city', 'facility', 'total_charges'
    , 'sum_svc_lines', 'holding_flag', 'reject_flag'
    , 'carrier_lob'
), file=sys.stderr) 

for line in sys.stdin:

    if not re.match(r'\s{0,6}{', line):
             continue

    dicy = json.loads(line)
    
    taxid = dicy['billing_provider_taxid']
    
    custid = dicy['cust_id']    
    pat_acct = dicy['pat_acct']

    try:
        claim_num = dicy['vx_carrier_claim_number']
    except:
        claim_num = ''

    try:
        name = dicy['vx_carrier_name']
    except:
        name = ''

    try:
        ky = dicy['vx_pm_sk']
    except:
        ky = ''

    claimtype = dicy['claim_type']

    try:
        holding = dicy['holding_flag']
    except:
        holding= ''
    try:
        reject = dicy['reject_flag']
    except:
        reject = '' 

    patcity = dicy['patient_city']

    try:
        facility = dicy['facility']
    except:
        facility = ''

    try:
        if dicy['holding_info'] != '':
            info = dicy['holding_info']
        else:
            info = dicy['reject_info']    
    except:
        info = ''
    
    try:
        lob = dicy['vx_carrier_lob']
    except:
        lob = ''

    print('{}|{} --- {}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}-{} --- {}'.format(
        info, taxid, claimtype, custid, pat_acct
        , claim_num, ky, name
        , patcity, facility, dicy['total_charges']
        , check_sum(dicy), holding, reject
        , lob 
    ), file=sys.stderr) 
    
    print(json.dumps(dicy))

