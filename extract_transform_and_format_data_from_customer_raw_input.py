#! /usr/bin/python3
import os
import sys
import re
import csv
import json

import codecs
import hashlib

from collections import Counter, defaultdict


"""
SV2*0636*HC|J2270*137.5*UN*11~^M
DTP*472*D8*20160118~^M
LIN**N4*00409189001~^M
CTP****.5*ML~    | price_per_measure / pricing_measure_quantity / pricing_measure_unit_name
"""


field_map = {
    #'self_subscriber'
    #'claim_filing_indicator'

    'ACCIDENT_DATE' :         'accident_date' ,
    'ACCIDENT_STATE':         'accident_state',
    'ADMISSION_DATE':         'admission_date',
    'ADMISSION_HOUR':         'admission_hour',
    'ADMIT_DIAG_CD' :         'admission_diagnosis',
    'ADMIT_TYPE'    :         'admission_type_code',

    'DISHARGE_HOUR':          'discharge_hour_minute',
    'AMT_PAID':               'payment',

    'ATT_PRV_NM_ID_FIRST_NAME':      'attending_physician_firstname'  ,
    'ATT_PRV_NM_ID_LAST_NAME':       'attending_physician_lastname'   ,
    'ATT_PRV_NM_ID_MIDDLE_NAME':     'attending_physician_middlename' ,
    'ATT_PRV_NM_ID_NPI':             'attending_physician_npi'        ,
    'ATT_PRV_NM_ID_STATE_LICENSE':   'attending_physician_state_license',
    'ATT_PRV_NM_ID_TAXONOMY':        'attending_physician_taxonomy'   ,

    'BEN_ASSIGN_IND':                'benefits_assignment'     ,
    'REL_INFO_IND':                  'release_of_information'  ,

    'BILL_PRV_ORG_NAME':             'billing_provider'        ,
    'BILL_PRV_ADDRESS_1':            'billing_provider_addr1'  ,
    'BILL_PRV_ADDRESS_2':            'billing_provider_addr2'  ,
    'BILL_PRV_CITY':                 'billing_provider_city'   ,
    'BILL_PRV_STATE':                'billing_provider_state'  ,
    'BILL_PRV_TAX_ID':               'billing_provider_taxid'  ,
    'BILL_PRV_ZIP':                  'billing_provider_zip'    ,
    'BILL_PRV_NPI':                  'billing_provider_npi'    ,
    'BILL_PRV_OTH_ID_1' :            'billing_provider_taxonomy' ,
    'BILL_PRV_PHONE'    :            'billing_provider_phone'  ,

    'DRG_CD':                        'DRG'          ,

    'EMPLOYER_NAME_1':               'employer'     ,
    'ESTIMATED_AMT_DUE_1':           'balance'      ,
    #'EXT_CAUSE_OF_INJ_CD_1':         'external_causes'   ,

    'FAC_CODE_PLACE_OF_SRVC':        'bill_type',
    'FIN_CLASS':                     'financial_class',

    'GUARANTOR_TYPE':                'guarantor_type',
    #'GROUP_NAME_1':                  'health_insurance_group'
    #'HEALTH_PLAN_ID_1':              'health_plan_id'

    #'INSURED_FIRST_NAME_1':          'subscriber_firstname'
    #'INSURED_LAST_NAME_1':           'subscriber_lastname'

    #'INSURED_UNIQUE_IDENTIFIER_1' :  'insurance_policy_id'
    #'INS_GROUP_NUMBER_1'    :        'insurance_group_number'
    #'INS_REL_TO_PATIENT_1'  :        'patient_subscriber_relation'

    'MEDICAL_RECORD_NUMBER':         'patient_mrn',

    #'OCC_CD_AND_DT_CODE_1':            'occurrences'
    #'OCC_CD_AND_DT_DATE_1':

    'OPR_PRV_NM_ID_FIRST_NAME':        'operating_physician_firstname',
    'OPR_PRV_NM_ID_LAST_NAME':         'operating_physician_lastname',
    'OPR_PRV_NM_ID_MIDDLE_NAME':       'operating_physician_middlename',
    'OPR_PRV_NM_ID_NPI':               'operating_physician_npi',
    'OPR_PRV_NM_ID_STATE_LICENSE':     'operating_physician_state_license',
    'OPR_PRV_NM_ID_TAXONOMY':          'operating_physician_taxonomy',

    #'OTHER_DIAG_CD_1':                 'other_diagnoses' ,

    'OTH_OPR_PRV_NM_ID_FIRST_NAME':    'oo_physician_firstname',
    'OTH_OPR_PRV_NM_ID_LAST_NAME':     'oo_physician_lastname',
    'OTH_OPR_PRV_NM_ID_MIDDLE_NAME':   'oo_physician_middlename',
    'OTH_OPR_PRV_NM_ID_NPI':           'oo_physician_npi',
    'OTH_OPR_PRV_NM_ID_STATE_LICENSE': 'oo_physician_state_license',
    'OTH_OPR_PRV_NM_ID_TAXONOMY':      'oo_physician_taxonomy',


    #'OTH_PROC_CDS_CODE_2' :            'other_procedures'

    'PATIENT_ACCOUNT_NUMBER':                'patient_account' ,
    'PATIENT_ADDRESS_1':                     'patient_addr1'   ,
    'PATIENT_ADDRESS_2':                     'patient_addr2'   ,
    'PATIENT_BIRTH_DATE':                    'patient_dob'     ,
    'PATIENT_CITY':                          'patient_city'    ,
    'PATIENT_DISCHARGE_STATUS':              'patient_status_code',
    'PATIENT_FIRST-NAME':                    'patient_firstname',
    'PATIENT_LAST_NAME':                     'patient_lastname' ,
    'PATIENT_MIDDLE_NAME':                   'patient_middlename',
    'PATIENT_PHONE':                         'patient_phone'  ,
    'PATIENT_SEX':                           'patient_gender' ,
    'PATIENT_SSN':                           'patient_ssn'    ,
    'PATIENT_STATE':                         'patient_state'  ,
    'PATIENT_ZIP':                           'patient_zip'    ,

    #'PAT_RSN_FOR_VISIT_1':                   'patient_reasons',
    #'PAYER_NAME_1':                          'payer_name'    ,

    'PAY_TO_ORG_NAME':                       'pay_to_name'    ,
    'PAY_TO_PRV_ADDRESS_1':                  'pay_to_addr1'   ,
    'PAY_TO_PRV_ADDRESS_2':                  'pay_to_addr2'   ,
    'PAY_TO_PRV_CITY':                       'pay_to_city'   ,
    'PAY_TO_PRV_STATE':                      'pay_to_state'  ,
    'PAY_TO_PRV_ZIP9':                       'pay_to_zip'    ,

    'PRNCPL_DIAG_CD':                        'principal_diagnosis' ,
    #'PRNCPL_PROC_CD_AND_DT_CODE_1':         'principal_procedure' ,
    #'PRNCPL_PROC_CD_AND_DT_DATE_1':

    'SOURCE_OF_ADMISSION':                   'admission_source_code',

    'SRV_FAC_LOC_NM_ID_FACILITY_NAME':       'facility',
    'SRV_FAC_LOC_NM_ID_ADDRESS1':            'facility_addr1',
    'SRV_FAC_LOC_NM_ID_ADDRESS2':            'facility_addr2',
    'SRV_FAC_LOC_NM_ID_CITY':                'facility_city',
    'SRV_FAC_LOC_NM_ID_NPI':                 'facility_npi',
    'SRV_FAC_LOC_NM_ID_STATE':               'facility_state',
    'SRV_FAC_LOC_NM_ID_STATE_TAXONOMY':      'facility_taxonomy',
    'SRV_FAC_LOC_NM_ID_ZIP':                 'facility_zip',

    'STATEMENT_FROM':                        'statement_from',
    'STATEMENT_THROUGH':                     'statement_through',

    'TOTAL_CLAIM_CHARGE_AMT':                'total_charges',

    #'TREAT_AUTH_NUMBER_1':             'prior_auth',       # ?
    #'VAL_CD_AND_AMT_CODE_1':           'value_information',
    #'VAL_CD_AND_AMT_VALUE_1':          'value_information'         

}


all_acct = defaultdict(dict)
acct_proc_index = defaultdict(int)


def get_delimiter(fname):
    with open(fname, 'r') as fh:
        header_line = next(fh).strip().replace(' ', '').replace("'", '').replace('"', '')
        header_line = header_line.upper()

    #-----------------------------------
    # basic header content checking
    #  1. cleaned length = 3626
    #  2. special field 'PATIENT_FIRST-NAME' with "-" and "_"
    #  3. 
    #-----------------------------------
    if ( len(header_line) != 3626            or
        'PATIENT_FIRST-NAME' not in header_line ):
        raise  ValueError("acct file header changed")

    m = hashlib.sha256()
    m.update(str.encode(header_line))
    chksum = m.hexdigest()
    if chksum != 'abb44246897ba34d1777ffbff21b58cc71e7d397a0603f5944206a9828632529':
        raise  ValueError("acct file header changed")

    counts = Counter(header_line)
    result = '|'
    for el in counts.most_common():
        if re.match(r'\w', el[0]):
             continue
        else:
             result = el[0]
             break

    return result


def just_date(dstr) :
    da = dstr.split(' ')[0]
    da = re.sub(r'\D', '', da)
    return da


def format_date(dstr) :
    if not re.search(r'\d', dstr):
        return ''
    da = dstr.strip().split(' ')[0]
    if re.match(r'\d\d/\d\d/\d{4}', da):
        parts = da.split('/')
        da = parts[2] + parts[0] + parts[1]
    return da


def format_money(x) :
    x = x.strip()
    negative = False
    if x[0] == '-' :
        negative = True
        x = x[1:]

    x = re.sub(r'^0+([1-9.])', r'\1', x)
    if x[0] == '.':
        x = '0' + x

    if negative:
        x = '-' + x

    return x


def get_header(record):
    result = { 'cust_id'     :  484,
               'claim_type'  : '837I',
               'holding_flag': 'N',
               'holding_info': '',
               'output_id'   : 1,
               'historical'  : 'N',
               'due_to_accident' : 'Y'}

    for k,v in record.items():
        if k in field_map :                     # only map fields interested
            result[field_map[k]] = v.strip()

    tmp_l = []
    for i in range(1, 13):
        fld = 'CONDITION_CODE_{}'.format(i)
        val = record.get(fld, '')
        if val:
            tmp_l.append(val)
    result['condition_information'] = ','.join(tmp_l)

    tmp_l = []
    for i in range(1, 13):
        fld1 = 'OCC_CD_AND_DT_CODE_{}'.format(i)
        code = record.get(fld1, '').strip()
        if code :
            fld2 = 'OCC_CD_AND_DT_DATE_{}'.format(i)
            date = record.get(fld2, '').strip()
            tmp_l.append('{}:{}'.format(code, format_date(date)))
    result['occurrences'] = ','.join(tmp_l)

    tmp_l = []
    for i in range(1, 13):
        fld1 = 'OCC_SP_CODE_{}'.format(i)
        code = record.get(fld1, '')
        if code :
            fld_d1 = 'OCC_SP_START_DATE_{}'.format(i)
            fld_d2 = 'OCC_SP_END_DATE_{}'.format(i)
            start = record.get(fld_d1, '')
            end   = record.get(fld_d2, '')
            tmp_l.append('{}:{}-{}'.format(code, start, end))
    result['occur_spans'] = ','.join(tmp_l)

    tmp_l = []
    for i in range(1, 13):
        fld1 = 'VAL_CD_AND_AMT_CODE_{}'.format(i)
        code = record.get(fld1, '').strip()
        if code:
            fld2 = 'VAL_CD_AND_AMT_VALUE_{}'.format(i)
            amt  = record.get(fld2, '').strip().lstrip('0')
            tmp_l.append('{}:{}'.format(code, amt))
    result['value_information'] = ','.join(tmp_l)

    tmp_l = []
    for i in range(1, 25):
        fld = 'OTHER_DIAG_CD_{}'.format(i)
        dx_code = record.get(fld, '').replace('.', '').strip()
        if dx_code :
            tmp_l.append(dx_code)
    result['other_diagnoses'] = ','.join(tmp_l)

    tmp_l = []
    for i in range(1, 4):
        fld = 'PAT_RSN_FOR_VISIT_{}'.format(i)
        reas = record.get(fld, '').strip()
        if reas :
            reas = reas.replace('.', '')
            tmp_l.append(reas)
    result['patient_reasons'] = ','.join(tmp_l)

    tmp_l = []
    for i in range(1, 13):
        fld = 'EXT_CAUSE_OF_INJ_CD_{}'.format(i)
        code = record.get(fld, '').replace('.', '').strip()
        if code :
            tmp_l.append(code)
    result['external_causes'] = ','.join(tmp_l)

    princ_code = record.get('PRNCPL_PROC_CD_AND_DT_CODE_1', '').strip()
    if princ_code:
        princ_code_date = record.get('PRNCPL_PROC_CD_AND_DT_DATE_1', '').strip()
        result['principal_procedure'] = '{}:{}'.format(princ_code, princ_code_date)
    else:
        result['principal_procedure'] = ''

    tmp_l = []
    for i in range(1, 13):
        fld1 = 'OTH_PROC_CDS_CODE_{}'.format(i)
        code = record.get(fld1, '').strip()
        #print(code)
        if code:
            fld2 = 'OTH_PROC_CDS_DATE_{}'.format(i)
            date = record.get(fld2, '').strip()
            tmp_l.append('{}:{}'.format(code, date))
    result['other_procedures'] = ','.join(tmp_l)


    """
     processing other insurance information
    """
    real_idx = 1
    for i in range(1, 6):
        payer_labels = ("PAYOR_NAME_{0}|HEALTH_PLAN_ID_{0}|PRIOR_PAYMENTS_{0}|ESTIMATED_AMT_DUE_{0}|"
                        "INSURED_LAST_NAME_{0}|INSURED_FIRST_NAME_{0}|PAT_REL_TO_INSURED_{0}|"
                        "GROUP_NAME_{0}|INS_GROUP_NUMBER_{0}|TREAT_AUTH_NUMBER_{0}|"
                        "DOC_CONTROL_NUMBER_{0}|EMPLOYER_NAME_{0}" ).format(i)
        flds = payer_labels.split('|')
        vals = [ (record.get(f,'')).strip() for f in flds ]

        tmp_str = vals[0] + vals[7]   # payor_name + group_name
        if re.search(r'Medlytix', tmp_str, re.I):
            json_labels =("OP{0}_payer_name|OP{0}_insurance_plan_id|OP{0}_payment|OP{0}_balance|OP{0}_sub_lastname|"
                         "OP{0}_sub_firstname|OP{0}_pat_relation|OP{0}_group_name|OP{0}_insurance_group_number|"
                         "OP{0}_REF_F8_info|OP{0}_claim_number|OP1_guarantor_employer").format(0)
            json_flds = json_labels.split('|')
            for j in range(len(json_flds)):
                fld = json_flds[j]
                if re.search(r'payment|balance', fld):
                    result[fld] = format_money(vals[j]).lstrip('-')
                else:
                    result[fld] = vals[j]

        elif any(vals):
            json_labels =("OP{0}_payer_name|OP{0}_insurance_plan_id|OP{0}_payment|OP{0}_balance|OP{0}_sub_lastname|"
                         "OP{0}_sub_firstname|OP{0}_pat_relation|OP{0}_group_name|OP{0}_insurance_group_number|"
                         "OP{0}_REF_F8_info|OP{0}_claim_number|OP1_guarantor_employer").format(real_idx)
            real_idx += 1
            json_flds = json_labels.split('|')
            for j in range(len(json_flds)):
                fld = json_flds[j]
                if re.search(r'payment|balance', fld):
                    result[fld] = format_money(vals[j]).lstrip('-')
                else:
                    result[fld] = vals[j]

    paid = 0.0
    for i in range(1, 6):
        item = 'OP{}_payment'.format(i)
        try:
            one_pay = float(result[item])
        except:
            continue
        else:
            paid += one_pay


    """
    formatting
    """
    result['accident_date']  = format_date(result['accident_date'])
    result['admission_date'] = format_date(result['admission_date'])
    result['patient_dob']    = format_date(result['patient_dob'])
    result['statement_from'] = format_date(result['statement_from'])
    result['statement_through'] = format_date(result['statement_through'])
    if not result['statement_through']:    # could be empty/missing
        result['statement_through'] = result['statement_from']
    result['statement_period']  = '{}-{}'.format(result['statement_from'],
                                                 result['statement_through'])

    result['balance'] = format_money(result['balance'])
    paid = max(paid, float(result['payment']))
    result['payment']  = '{:.2f}'.format(paid)
    result['total_charges'] = format_money(result['total_charges'])

    x = re.sub(r'\D', '', result['discharge_hour_minute'])
    if len(x) in [6, 14]:
        result['discharge_hour_minute'] = x[-6:-2]
    elif len(x) == 4:
        result['discharge_hour_minute'] = x

    result['statement_period'] = '{}-{}'.format(result['statement_from'], result['statement_through'])
    result['claim_frequency'] = result['bill_type'][-1]
    result['facility_code']   = result['bill_type'][:2]

    result['principal_diagnosis'] = result['principal_diagnosis'].replace('.', '')
    result['admission_diagnosis'] = result['admission_diagnosis'].replace('.', '')
    result['other_diagnoses']     = result['other_diagnoses'].replace('.', '')
    result['patient_reasons']     = result['patient_reasons'].replace('.', '')
    result['external_causes']     = result['external_causes'].replace('.', '')

    if '999999' in result['patient_phone']:
        result['patient_phone'] = ''

    return result


"""
def get_proc(record, idx):
    proc_map = {'SVC_LINE_NUMBER' :   'line_ctrlnum',
                'SVC_REVENUE_CODE':   'revenue_code',
                'SVC_DATE'        :   'service_date',
                'SVC_HCPC'        :   'cpt_code',
                'SVC_NDC'         :   'drug_code',
                'SVC_TOTAl_CHARGE':   'charge',
                'SVC_QUANTITY'    :   'unit_quantity',
                'SVC_UNIT_BASIS_CODE':'unit_name',
                'SVC_UNITS'       :   'x_unit',
                'SVC_DESCRIPTION' :   'svc_desc',
                'PROCEDURE_MODIFIER1': 'modifier1',
                'PROCEDURE_MODIFIER2': 'modifier2',
                'PROCEDURE_MODIFIER3': 'modifier3',
                'PROCEDURE_MODIFIER4': 'modifier4',
                'ACCOUNT_NUMBER':      'patient_account',
                }

    prefix = 'LX{:0>3}_'.format(idx)    # padding up to three zeros from the left
    ret = {}

    for l, r in proc_map.items():
        if '_date' in r:
            ret[prefix + r] = format_date(record[l])
        else:
            ret[prefix + r] = record.get(l, '')

    return ret
"""


def parse( data_fh, deli, outfh = sys.stdout) :
    rec_src = csv.reader(data_fh, delimiter=deli, quotechar = '"')
    fields = next(rec_src)
    data_file = os.path.basename(data_fh.name)
    for row in rec_src:
        #print(row[15])
        result = {}
        rec_dic = dict(zip(fields,row))
        for k,v in rec_dic.items():
            if k in field_map :                     # only map fields interested
                result[field_map[k]] = v.strip()

        acct_key ='{}-{}-{}'.format(result['patient_account'],
                                    format_date(result['statement_from']),
                                    format_date(result['statement_through']))

        if acct_key not in all_acct:
            all_acct[acct_key] = get_header(rec_dic)

    for _, v in all_acct.items():
        if v['attending_physician_lastname'] == '':
            continue
        v['admission_date_time'] = v['admission_date'] + v['admission_hour'] + '00'
        v['balance'] = v['total_charges']
        v['data_file'] = data_file
        r = json.dumps(v)
        print(r, file=outfh)

    outfh.flush()


if __name__ == '__main__' :

    try:
        placement = sys.argv[1]
    except:
        sys.exit(1)
    else:
        delim = get_delimiter(placement)
        infh = codecs.open(placement, encoding='latin-1')
        parse(infh, delim)

