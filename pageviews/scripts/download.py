from __future__ import print_function

import os
import re
import ssl
import subprocess
import urllib2

url_base = 'https://dumps.wikimedia.org/other/pageviews'
year = '2019'
month = '01'

DATADIR = '../../../data/pageviews'

url = '{0}/{1}/{1}-{2}/'.format(url_base, year, month)

context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
h = urllib2.urlopen(url, context=context)
data = h.read()

filenames = []
matches = re.findall(r'(pageviews-\d+-\d+.gz)', data, re.DOTALL)
for m in matches:
    if m not in filenames:
        filenames.append(m)

for file in filenames:
    r = urllib2.urlopen('{0}/{1}'.format(url, file), context=context)
    outfile = os.path.join(DATADIR, file)
    if os.path.isfile(outfile):
        try:
            subprocess.check_call(['gzip', '-t', outfile])
            continue
        except:
            pass

    print(file)
    with open(outfile, 'w') as fp:
        fp.write(r.read())
