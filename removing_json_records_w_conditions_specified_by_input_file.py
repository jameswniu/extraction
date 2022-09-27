#! /usr/bin/python3
import json
import sys
import re
from collections import defaultdict


# identify lines to remove
cnt = defaultdict(int)
k = {}
with open('template.txt', 'r') as fh:
    for line in fh:
        # remove invalid addr and zip
        if re.match(r'I\d{7}[|]', line) and 'to change' not in line and 'to add' not in line:
            pm_sk = re.search(r'\d{7}', line).group()
            k[pm_sk] = f'{line.strip()} (removed)'

            cnt['no found insured zip'] += 1
    
        # remove unfindable dx_code_desc
        if re.match(r'\d{7}\s:\sno\sdx_code_desc', line):
            pm_sk = re.search(r'\d{7}', line).group()
            k[pm_sk] = f'{line.strip()} (removed)'

            cnt['no dx_code_desc (full)'] += 1

        # remove other issues
        if re.match(r'\d{7}:\s\w+\b', line):
            pm_sk = re.search(r'\d{7}', line).group()
            k[pm_sk] = f'{line.strip()} (removed)'

            cnt['other issues'] += 1
        
# remove associated pm_sk
for line in sys.stdin:
    if not re.match(r'\s{0,6}{', line):
        continue

    dicy = json.loads(line)
    if dicy['vx_pm_sk'] in k:
        #print(k[dicy['vx_pm_sk']], file=sys.stderr)        

        continue

    print(json.dumps(dicy))

# print summary
for prob in cnt:  
    print("""removed {} {}""".format(prob, cnt[prob]), file=sys.stderr)

