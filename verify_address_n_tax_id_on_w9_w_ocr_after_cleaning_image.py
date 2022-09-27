import os, sys
import re
import json
import shutil
import pytz
import numpy as np

import pytesseract
import cv2

from copy import deepcopy, copy
from datetime import datetime as dt, timedelta

from glob import glob

from pdf2image import convert_from_path
from PIL import Image
from pytesseract import Output

#----
# Remove lines around alphanumerical characters to increase reading accuracy
#----
def removelines(name):
    image = cv2.imread(name)
    result = image.copy()

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

    # Remove horizontal lines
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (25, 1))
    remove_horizontal = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
    cnts = cv2.findContours(remove_horizontal, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]
    for c in cnts:
        cv2.drawContours(result, [c], -1, (255, 255, 255), 5)

    # Remove vertical lines
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 15))
    remove_vertical = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, vertical_kernel, iterations=4)
    cnts = cv2.findContours(remove_vertical, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]
    for c in cnts:
        cv2.drawContours(result, [c], -1, (255, 255, 255), 5)

    return result


#----
# Enhance badly printed/defined alphanumerical characters using Gaussian Blur
#----
def contrt(name):
    image = cv2.imread(name)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

    # Find contours and filter using contour area
    cnts = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]
    for c in cnts:
        area = cv2.contourArea(c)
        if area > 500:
            cv2.drawContours(thresh, [c], -1, 0, -1)

    # Invert image and OCR
    invert = 255 - thresh
    data = pytesseract.image_to_string(invert, lang='eng', config='--psm 6')

    return thresh


os.chdir(r'L:\Auto_Opportunity_Analysis\w9s_with_Medistreams_Lockbox_09012021')


c, d = 0, 0
j = 0
reso = 500    ## SPECIFY medium resolution not sacrificing quality and storage

for f in glob('*.pdf'):
    if j > 999:
        break

    pages = convert_from_path(f, reso)

    i = 0
    for page in pages:
        if i > 0:
            break

        name1 = 'r' + f.replace('pdf', 'jpg')
        page.save(name1, 'JPEG')

    p1 = removelines(name1)
    cv2.imwrite(name1.replace('r', 'p'), p1)
    p2 = removelines(name1.replace('r', 'p'))
    cv2.imwrite(name1.replace('r', 'f'), p2)

    os.remove(name1)
    os.remove(name1.replace('r', 'p'))

    i += 1

    # fw0 = open(f.replace('pdf', 'txt'), 'w')

    text = pytesseract.image_to_string(Image.open(name1.replace('r', 'f')))

    os.remove(name1.replace('r', 'f'))

    # print(text, file=fw0)

    # fw0.close()

    print(f)
    print()

    try:
        idind = 'n'

        pobox = '{}, {}'.format(re.search(r'PO.*BOX.*', text).group()
                                , re.search(r'ATLANTA.*GA.\d{5}.\d{4}', text).group())

        if 'PO BOX 740011' and 'ATLANTA, GA 30374-0011' in pobox:
            poboxind = 'y'

            c += 1

        else:
            poboxind = 'n'

        print('{} | {}'.format(pobox, poboxind))
    except:
        poboxind = 'n'

        print('PO BOX not found')

    try:
        for line in text.split('\n'):
            idind = 'n'

            digits = ''.join(re.findall('\d+', line))

            if len(digits) == 9 and digits != '303740011':
                if digits in ''.join(re.findall('\d+', f)):
                    idind = 'y'

                    print('{} | {}'.format(digits, idind))

                    d += 1

                    break
                else:
                    idind = 'n'

                    print('{} | {}'.format(digits, idind))
    except:
        idind = 'n'

        print('tax id not found')

    print(poboxind, idind)

    #----
    # separate into folders based on amount of info verified
    #----
    if poboxind == 'y' and idind == 'y':
        os.rename(f, r'{}\verified\{}'.format(os.getcwd(), f))

        print('moved to verified...')

    if poboxind == 'y' and idind == 'n':
        os.rename(f, r'{}\verified_addr\{}'.format(os.getcwd(), f))

        print('move to verified_addr...')

    print('-' * 200)

    j += 1
print('reso {} : verified addr {} | verified id {} | total {}'.format(reso, c, d, j))

