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

# list the tabulated datasets
directory_1 = stream+'-subset/'
if testing == '09232017-09242017':
    list_tweet = ['A-processed-09232017-09232017-subset-'+set_type+'-'+set_list+'_tabulated-tweet.csv.gz',
                  'A-processed-09232017-09242017-subset-'+set_type+'-'+set_list+'_tabulated-tweet.csv.gz']
    list_user = ['A-processed-09232017-09232017-subset-'+set_type+'-'+set_list+'_tabulated-user.csv.gz',
                 'A-processed-09232017-09242017-subset-'+set_type+'-'+set_list+'_tabulated-user.csv.gz']
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

# make directory for merged data
directory_0 = stream+'-merged/'
try:
	os.makedirs(directory_0)
except FileExistsError:
	pass

data_label_merge = stream+'-processed-'+testing+'-subset-'+set_type+'-'+set_list
print('--------------------------------------------------')
print('### mergeTweets.py '+data_label_merge+' ###')
print()

start_1 = time.time()
print('Merging '+list_tweet[0]+'...')
tb_main = pd.read_csv(directory_1+list_tweet[0],compression='gzip',sep=',',index_col=0,header=0,dtype=str)
tb_UID_main = pd.read_csv(directory_1+list_user[0],compression='gzip',sep=',',index_col=0,header=0,dtype=str)
end_1 = time.time()
print('Finished reading '+list_tweet[0]+'!')
print('Computing time: '+str(round(end_1-start_1,2))+' seconds.')
print()
for k, i in enumerate(list_tweet):
    if k == 0:
        pass
    else:
        start_1 = time.time()
        print('Merging '+i+'...')
        # tweets
        tb = pd.read_csv(directory_1+i,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
        idks = np.array(tb['TXT'].keys())
        # get tweet ids
        will_add = [] # new ids
        existing = [] # existing
        for c, idk in enumerate(idks):
            try:
                test_main = tb_main['TMRT'][idk]
                existing.append(idk)
            except:
                will_add.append(idk)
        # add new ids into the main table
        tb_will_add = tb.loc[will_add,:]
        tb_merged = tb_main.append(tb_will_add)
        tb_main = tb_merged
        # merge existing ids and add to main table
        columns_label = ['RTT','RPT','TRP','NRT',
    					 'PTID','UID','USN','TM',
    					 'RPC','FVC','RTC','L',
    					 'TXT','HTGS','UMS',
    					 'TMRT','RPCRT','FVCRT','RTCRT',
    					 'UIDRT','USNRT']
        for j in existing:
            for jj in columns_label:
                if jj in ['TMRT','RPCRT','FVCRT','RTCRT','UIDRT']:
                    main_val = tb_main[jj][j]
                    val = tb[jj][j]
                    if main_val != val:
                        tb_main[jj][j] = main_val+','+val
                else:
                    main_val = tb_main[jj][j]
                    val = tb[jj][j]
                    if main_val != val:
                        tb_main[jj][j] = val
        del tb, idks, will_add, existing, tb_will_add, tb_merged
        # users
        tb_UID = pd.read_csv(directory_1+list_user[k],compression='gzip',sep=',',index_col=0,header=0,dtype=str)
        uidks = np.array(tb_UID['UT'].keys())
        # get user ids
        will_add = []
        existing = []
        for c, uidk in enumerate(uidks):
            try:
                test_main = tb_UID_main['UT'][uidk]
                existing.append(uidk)
            except:
                will_add.append(uidk)
        # add new user ids into the main tables
        tb_UID_will_add = tb_UID.loc[will_add,:]
        tb_UID_merged = tb_UID_main.append(tb_UID_will_add)
        tb_UID_main = tb_UID_merged
        # merge existing user ids and add to main table
        columns_label = ['USN','UT','UNFL']
        for j in existing:
            for jj in columns_label:
                main_val = tb_UID_main[jj][j]
                val = tb_UID[jj][j]
                if main_val != val:
                    tb_UID_main[jj][j] = main_val+','+val
        del tb_UID, uidks, will_add, existing, tb_UID_will_add, tb_UID_merged
        end_1 = time.time()
        print('Computing time: '+str(round(end_1-start_1,2))+' seconds.')
        print()

# Save main table
start_0 = time.time()
print('Saving...')
# save merged tables
tb_main.to_csv(directory_0+data_label_merge+'_tabulated-tweet.csv.gz',sep=',',compression='gzip')
tb_UID_main.to_csv(directory_0+data_label_merge+'_tabulated-user.csv.gz',sep=',',compression='gzip')
del tb_main, tb_UID_main
end_0 = time.time()
print('Computing time: '+str(round(end_0-start_0,2))+' seconds.')
print()
