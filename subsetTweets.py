#!/usr/bin/env python3

##!/home/[username]/miniconda3/bin/python

### Takes a specific subset of the tabulated data
## Alex John Quijano
## Created: 3/25/2014
## Updated: 3/25/2014

import twitter as tw
import numpy as np
import pandas as pd
import time
import math
import sys
import os
import re

stream = sys.argv[1]
key_type = sys.argv[2]
key_list = sys.argv[3]
testing = sys.argv[4]
mod = int(sys.argv[5])

# make directory for subset data
directory_0 = stream+'-subset/'
try:
	os.makedirs(directory_0)
except FileExistsError:
	pass

# directory of tabulated tweets
directory_1 = stream+'-tabulated/'
tabulated_tweet = directory_1+'A-processed-'+testing+'_tabulated-tweet.csv.gz'
tabulated_user = directory_1+'A-processed-'+testing+'_tabulated-user.csv.gz'

# list subset keywords
directory_2 = 'keywords-subset/'
file = open(directory_2+'keywords-'+key_type+'-'+key_list+'.txt')
keywords = []
for f in file:
    keywords.append(f.lower().replace('\n',''))
file.close()

# subset and merge tables
data_label_subset = stream+'-processed-'+testing+'-subset-'+key_type+'-'+key_list
print('--------------------------------------------------')
print('### subsetTweets.py '+data_label_subset+' ###')
print()

if key_type == 'hashtag':
    field = 'HTGS'
elif key_type == 'user-mention':
    field = 'UMS'
else:
    field = 'HTGS' # default field

# list subset related keywords (hashtags and user_mentions/users)
def indexer(table,j,keys):
	FF = str(table[field][j]).lower().split(',')
	HTGS = str(table['HTGS'][j]).lower().split(',')
	UMS = str(table['UMS'][j]).lower().split(',')
	UID = table['UID'][j]
	USN = table['USN'][j]
	PTID = table['PTID'][j]
	if len(set(keys) & set(FF)) > 0:
		return HTGS, UMS, UID, USN, PTID
	else:
		return None
h = []
um = []
ui = []
us = []
pi = []
tb_list = []
tb_UID_list = []
start_1 = time.time()
print('Indexing '+tabulated_tweet+'...')
tb = pd.read_csv(tabulated_tweet,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
tb_UID = pd.read_csv(tabulated_user,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
tb_list.append(tb)
tb_UID_list.append(tb_UID)
idks = np.array(tb[field].keys())
for c, idk in enumerate(idks):
	try:
		FF = str(tb[field][idk]).lower().split(',')
		HTGS = str(tb['HTGS'][idk]).lower().split(',')
		UMS = str(tb['UMS'][idk]).lower().split(',')
		UID = tb['UID'][idk]
		USN = tb['USN'][idk]
		PTID = tb['PTID'][idk]
		if len(set(keywords) & set(FF)) > 0:
			if HTGS[0] != '*':
				h.extend(HTGS)
			if UMS[0] != '*':
				um.extend(UMS)
			if UID != '*':
				ui.append(UID)
			if USN != '*':
				us.append(USN)
			if PTID != '*':
				pi.append(PTID)
		if c%mod == 0:
			print('...iteration ('+str(round((c/len(idks))*100,2))+'%) \t '+str(c)+'/'+str(len(idks))+' (TID '+str(idk)+') S for Success!...')
	except:
		print('...iteration ('+str(round((c/len(idks))*100,2))+'%) \t '+str(c)+'/'+str(len(idks))+' (TID '+str(idk)+') F for Failure!...')
end_1 = time.time()
print('Finished indexing '+tabulated_tweet+'!')
print('Computing time: '+str(round(end_1-start_1,2))+' seconds.')
print()

start_1 = time.time()
print('Preparing related keywords...')
h = np.unique(h)
set_h = set(h)
um = np.unique(um)
set_um = set(um)
ui = np.unique(ui)
set_ui = set(ui)
us = np.unique(us)
set_us = set(us)
pi = np.unique(pi)
set_pi = set(pi)
print('Computing time: '+str(round(end_1-start_1,2))+' seconds.')
print()

# Extract and merge subsets according to the related keywords
tb_subset = []
tb_UID_subset = []
start_1 = time.time()
print('Subsetting '+tabulated_tweet+'...')
tb_rows = []
tb_UID_rows = []
idks = np.array(tb[field].keys())
for c, idk in enumerate(idks):
	try:
		row = ['*']*2
		row_UID = ['*']*2
		HTGS = str(tb['HTGS'][idk]).lower().split(',')
		UMS = str(tb['UMS'][idk]).lower().split(',')
		UID = tb['UID'][idk]
		USN = tb['USN'][idk]
		PTID = tb['PTID'][idk]
		if len(set_h & set(HTGS)) > 0:
			row[0] = idk
			row_UID[0] = UID
		if len(set_um & set(UMS)) > 0:
			row[0] = idk
			row_UID[0] = UID
		if len(set_ui & set(UID)) > 0:
			row[0] = idk
			row_UID[0] = UID
		if len(set_us & set(USN)) > 0:
			row[0] = idk
			row_UID[0] = UID
		if len(set_pi & set(PTID)) > 0:
			row[0] = idk
			row_UID[0] = UID
			try:
				test = table['UID'][int(PTID)]
				row[1] = int(PTID)
				row_UID[1] = table['UID'][int(PTID)]
			except:
				pass
		for oi_0 in row:
			if oi_0 != '*':
				tb_rows.append(int(oi_0))
		for oi_1 in row_UID:
			if oi_1 != '*':
				tb_UID_rows.append(int(oi_1))
		if c%mod == 0:
			print('...iteration ('+str(round((c/len(idks))*100,2))+'%) \t '+str(c)+'/'+str(len(idks))+' (TID '+str(idk)+') S for Success!...')
	except:
		print('...iteration ('+str(round((c/len(idks))*100,2))+'%) \t '+str(c)+'/'+str(len(idks))+' (TID '+str(idk)+') F for Failure!...')
tb_rows_C = np.unique(tb_rows)
tb_UID_rows_C = np.unique(tb_UID_rows)
subset = tb.loc[tb_rows_C,:]
subset_UID = tb_UID.loc[tb_UID_rows_C,:]

del tb, tb_UID, tb_rows, tb_UID_rows, tb_rows_C, tb_UID_rows_C
end_1 = time.time()
print('Finished subsetting '+tabulated_tweet+'!')
print('Computing time: '+str(round(end_1-start_1,2))+' seconds.')
print()

# save tables
start_0 = time.time()
#try:
print('Saving...')
subset.to_csv(directory_0+data_label_subset+'_tabulated-tweet.csv.gz',sep=',',compression='gzip')
subset_UID.to_csv(directory_0+data_label_subset+'_tabulated-user.csv.gz',sep=',',compression='gzip')
del subset, subset_UID
print('Finished saving!')
#except:
#	print('Error!')
end_0 = time.time()
print('Computing time: '+str(round(end_0-start_0,2))+' seconds.')
print()
