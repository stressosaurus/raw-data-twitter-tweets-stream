#!/usr/bin/env python3

##!/home/[username]/miniconda3/bin/python

### Converts the main data into table form
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

stream = sys.argv[1]
date_range = sys.argv[2]

data_label = stream+'-processed-'+date_range
data_label_subset = stream+'-processed-'+date_range

## Make a directory for the tabilized tweets
directory_1 = stream+'-tabulated/'
try:
	os.makedirs(directory_1)
except FileExistsError:
	pass

# tabulate tweets
start = time.time()
print('--------------------------------------------------')
print('### tabulateTweets.py '+data_label_subset+' ###')
print()

try:
	data, tweet_id_sets = tw.read(stream,date_range)
	data_tabulated, data_tabulated_UID = tw.tabulate(data,tweet_id_sets)
	data_tabulated.to_csv(directory_1+data_label_subset+'_tabulated-tweet.csv.gz',sep=',',compression='gzip')
	data_tabulated_UID.to_csv(directory_1+data_label_subset+'_tabulated-user.csv.gz',sep=',',compression='gzip')
	del data, tweet_id_sets, data_tabulated, data_tabulated_UID
	print('Finished tabulating!')
except:
	print('Error!')
end = time.time()
print('Computing time: '+str(round(end-start,2))+' seconds.')
print()
