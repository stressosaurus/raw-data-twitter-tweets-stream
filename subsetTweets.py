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
set_type = sys.argv[2]
set_list = sys.argv[3]
testing = sys.argv[4]
mod = int(sys.argv[5])
rk = sys.argv[6]

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
related_keywords = np.load(directory_2+'keywords-'+set_type+'-'+set_list+'-related-keywords-'+rk+'.npy',allow_pickle=True).item()
h = related_keywords['h']
um = related_keywords['um']
pi = related_keywords['pi']

if set_type == 'hashtag':
    field = 'HTGS'
elif set_type == 'user-mention':
    field = 'UMS'
else:
    field = 'HTGS' # default field

# subset and merge tables
data_label_subset = stream+'-processed-'+testing+'-subset-'+set_type+'-'+set_list
print('--------------------------------------------------')
print('### subsetTweets.py '+data_label_subset+' ###')
print()

def common_elements(list_a,list_b):
	result = False
	for y in list_b:
		if y in list_a:
			result = True
			return result
	return result

# Extract subsets according to the related keywords
start_1 = time.time()
print('Subsetting '+tabulated_tweet+'...')
tb = pd.read_csv(tabulated_tweet,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
tb_UID = pd.read_csv(tabulated_user,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
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
		if common_elements(h,HTGS) == True:
			row[0] = idk
			row_UID[0] = UID
		if common_elements(um,UMS) == True:
			row[0] = idk
			row_UID[0] = UID
		if common_elements(um,USN) == True:
			row[0] = idk
			row_UID[0] = UID
		if common_elements(pi,PTID) == True:
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
