#!/usr/bin/env python3

##!/home/[username]/miniconda3/bin/python

### This script merges tables
## Alex John Quijano
## Created: 3/4/2014
## Updated: 3/4/2014

import twitter as tw
import numpy as np
import pandas as pd
import time
import math
import sys
import os
import re

stream = sys.argv[1]
set_type = sys.argv[2]
set_list = sys.argv[3]
testing = sys.argv[4]
mod = int(sys.argv[5])
rk = sys.argv[6]

# list the tabulated datasets
directory_1 = stream+'-tabulated/'
if testing == '09232017-09242017':
    list_tweet = ['A-processed-09232017-09232017_tabulated-tweet.csv.gz',
                  'A-processed-09232017-09242017_tabulated-tweet.csv.gz']
    list_user = ['A-processed-09232017-09232017_tabulated-user.csv.gz',
                 'A-processed-09232017-09242017_tabulated-user.csv.gz']
else:
    list = os.listdir(directory_1)
    list_tweet = []
    list_user = []
    for i in list:
        if 'tweet' in i:
            list_tweet.append(i)
        elif 'user' in i:
            list_user.append(i)
list_tweet.sort()
list_user.sort()

# list subset keywords
directory_2 = 'keywords-subset/'
file = open(directory_2+'keywords-'+set_type+'-'+set_list+'.txt')
keywords = []
for f in file:
    keywords.append(f.lower().replace('\n',''))
file.close()

# index tweets
data_label_subset = stream+'-processed-'+testing+'-subset-'+set_type+'-'+set_list
print('--------------------------------------------------')
print('### indexTweets.py '+data_label_subset+' ###')
print()

if set_type == 'hashtag':
    field = 'HTGS'
elif set_type == 'user-mention':
    field = 'UMS'
else:
    field = 'HTGS' # default field

# list subset related keywords (hashtags and user_mentions/users)
def common_elements(list_a,list_b):
	result = False
	for y in list_b:
		if y in list_a:
			result = True
			return result
	return result
h = []
um = []
pi = []
for k, i in enumerate(list_tweet):
    start_1 = time.time()
    print('Indexing '+i+'...')
    tb = pd.read_csv(directory_1+i,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
    tb_UID = pd.read_csv(directory_1+list_user[k],compression='gzip',sep=',',index_col=0,header=0,dtype=str)
    idks = np.array(tb[field].keys())
    for c, idk in enumerate(idks):
    	#try:
    		FF = str(tb[field][idk]).lower().split(',')
    		HTGS = str(tb['HTGS'][idk]).lower().split(',')
    		UMS = str(tb['UMS'][idk]).lower().split(',')
    		PTID = tb['PTID'][idk]
    		if common_elements(keywords,FF) == True:
    			if HTGS[0] != '*':
    				h.extend(HTGS)
    			if UMS[0] != '*':
    				um.extend(UMS)
    			if PTID != '*':
    				pi.append(PTID)
    		if c%mod == 0:
    			print('...iteration ('+str(round((c/len(idks))*100,2))+'%) \t '+str(c)+'/'+str(len(idks))+' (TID '+str(idk)+') S for Success!...')
    	#except:
    	#	print('...iteration ('+str(round((c/len(idks))*100,2))+'%) \t '+str(c)+'/'+str(len(idks))+' (TID '+str(idk)+') F for Failure!...')
    end_1 = time.time()
    print('Finished indexing '+i+'!')
    print('Computing time: '+str(round(end_1-start_1,2))+' seconds.')
    print()

start_1 = time.time()
print('Preparing related keywords...')
h = np.unique(h)
um = np.unique(um)
pi = np.unique(pi)
end_1 = time.time()
print('Computing time: '+str(round(end_1-start_1,2))+' seconds.')
print()

start_1 = time.time()
print('Saving related keywords...')
related_keywords = {}
related_keywords['h'] = h
related_keywords['um'] = um
related_keywords['pi'] = pi
np.save(directory_2+'keywords-'+set_type+'-'+set_list+'-related-keywords-'+rk+'.npy',related_keywords)
del related_keywords
end_1 = time.time()
print('Computing time: '+str(round(end_1-start_1,2))+' seconds.')
print()
