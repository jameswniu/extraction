import os
import glob
import sys
import re

from datetime import datetime

from usps import USPSApi, Address


os.chdir(r'L:\Billing_Processing\logs')

dt = datetime.now()
Ymd = dt.strftime('%Y') + dt.strftime('%m') + dt.strftime('%d')


dicy = {}
cnt = 0
with open("""{}.txt""".format(Ymd), 'r') as fh:
    for line in fh:
        if not re.match(r'^I\d{7}', line):
            continue

        if 'to add zip' in line:
            continue

        tmp = [x.strip() for x in line.split('|')]
        pm_sk = tmp[0]
        adr = tmp[1]
        dicy[pm_sk] = adr

        cnt += 1
# print(dicy)
# print(cnt)


c0 = 0
c1 = 0
for k, v in dicy.items():
    l = v.split(',')
    # print(l)

    try:
        address = Address(
            name='',
            address_1=l[0],
            city=l[1],
            state=l[2],
            zipcode=''
        )

        usps = USPSApi('748MEDLY7441', test=True)
        validation = usps.validate_address(address)

        zp = validation.result['AddressValidateResponse']['Address']['Zip5']
        info = """(to add zip {})""".format(zp)

        c0 += 1
    except:
        info = ''

        c1 += 1

    print("""{}|{} {}""".format(k, v, info))

print('changed ' + str(c0))
print('remaining ' + str(c1))

