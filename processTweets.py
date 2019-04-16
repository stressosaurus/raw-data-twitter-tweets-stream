#!/usr/bin/env python3

##!/home/[username]/miniconda3/bin/python

### Twitter Data processor
## Alex John Quijano
## Created: 8/16/2018
## Updated: 8/30/2018

import os
import re
import sys
import json
import time
import tarfile
import numpy as np
from sklearn.externals.joblib import Parallel, delayed

# twitter chosen dates
STDVID = sys.argv[1]
dates = sys.argv[2].split(' ')
num_cores = int(sys.argv[3])

def format_boolean(boolean):
	boolean = str(boolean).lower()
	if boolean == 'true':
		return 1
	elif boolean == 'false':
		return 0
	else:
		return ''

def format_date(date):
	# Input format: Sun Sep 24 02:56:44 +0000 2017
	date = date.split(' ')
	month = date[1]
	day = date[2]
	year = date[5]
	time = date[3]
	output = month+' '+day+' '+year+' '+time+' +0'
	# Output format: Sep 24 2017 02:56:44 +0
	return output

def format_text(text):
	rsttext_0 = re.sub(r'http\S+', '', text)
	rsttext_0 = re.sub(r'\n+',' ', rsttext_0)
	return rsttext_0

# user information objects for organizer function
user_id_key = {}
user_id_key['screen_name'] = {}
user_id_key['verified'] = {}
user_id_key['location'] = {}
user_id_key['created_at'] = {}
user_id_key['followers_count'] = {}
user_id_key['friends_count'] = {}
user_id_key['tweet_id'] = {}

# tweet information objects for organizer function
tweet_id_key = {}
tweet_id_key['user_id'] = {}
tweet_id_key['tweet_id'] = {}
tweet_id_key['text'] = {}
tweet_id_key['time'] = {}
tweet_id_key['hashtags'] = {}
tweet_id_key['user_mentions'] = {}
tweet_id_key['language'] = {}
tweet_id_key['rfr_count'] = {}

tweet_id_key['user_id']['parent'] = {}
tweet_id_key['user_id']['retweet'] = {}
tweet_id_key['user_id']['reply'] = {}
tweet_id_key['tweet_id']['parent'] = {}
tweet_id_key['tweet_id']['retweet'] = {}
tweet_id_key['tweet_id']['reply'] = {}
tweet_id_key['tweet_id']['reply-reverse'] = {}
tweet_id_key['text']['parent'] = {}
tweet_id_key['text']['retweet'] = None
tweet_id_key['text']['reply'] = {}
tweet_id_key['time']['parent'] = {}
tweet_id_key['time']['retweet'] = {}
tweet_id_key['time']['reply'] = {}
tweet_id_key['hashtags']['parent'] = {}
tweet_id_key['hashtags']['retweet'] = None
tweet_id_key['hashtags']['reply'] = {}
tweet_id_key['user_mentions']['parent'] = {}
tweet_id_key['user_mentions']['retweet'] = None
tweet_id_key['user_mentions']['reply'] = {}
tweet_id_key['language']['parent'] = {}
tweet_id_key['language']['retweet'] = None
tweet_id_key['language']['reply'] = {}
tweet_id_key['rfr_count']['parent'] = {}
tweet_id_key['rfr_count']['retweet'] = {}
tweet_id_key['rfr_count']['reply'] = {}

def organizer(k):
	### input k is an opened json object - one line/onetweet
	tweet = json.loads(k)

	### if tweet is normal
	if tweet['tweet']['in_reply_to_status_id'] == None and tweet['retweeted_status'] == None:
		### user information
		prid = int(tweet['user']['id'])
		if prid not in user_id_key['screen_name'].keys():
			user_id_key['screen_name'][prid] = ''
		if prid not in user_id_key['verified'].keys():
			user_id_key['verified'][prid] = ''
		if prid not in user_id_key['location'].keys():
			user_id_key['location'][prid] = ''
		if prid not in user_id_key['created_at'].keys():
			user_id_key['created_at'][prid] = ''
		if prid not in user_id_key['followers_count'].keys():
			user_id_key['followers_count'][prid] = []
		if prid not in user_id_key['friends_count'].keys():
			user_id_key['friends_count'][prid] = []
		if prid not in user_id_key['tweet_id'].keys():
			user_id_key['tweet_id'][prid] = []
		user_id_key['screen_name'][prid] = tweet['user']['screen_name']
		user_id_key['verified'][prid] = format_boolean(tweet['user']['verified'])
		user_id_key['location'][prid] = tweet['user']['location']
		user_id_key['created_at'][prid] = format_date(tweet['user']['created_at'])
		user_id_key['followers_count'][prid].append(tweet['user']['followers_count'])
		user_id_key['friends_count'][prid].append(tweet['user']['friends_count'])
		user_id_key['tweet_id'][prid].append(tweet['tweet']['id'])

		### tweet information
		## parent tweet id
		prid = int(tweet['tweet']['id'])
		# user id
		tweet_id_key['user_id']['parent'][prid] = int(tweet['user']['id'])
		# tweet id
		tweet_id_key['tweet_id']['parent'][prid] = prid
		# text
		tweet_id_key['text']['parent'][prid] = format_text(tweet['tweet']['text'])
		# time
		tweet_id_key['time']['parent'][prid] = format_date(tweet['tweet']['created_at'])
		# hashtags
		tweet_id_key['hashtags']['parent'][prid] = tweet['tweet']['hashtags']
		# user_mentions
		tweet_id_key['user_mentions']['parent'][prid] = tweet['tweet']['user_mentions']
		# language
		tweet_id_key['language']['parent'][prid] = tweet['tweet']['lang']
		# reply-favorite-retweet (rfr) counts
		tweet_id_key['rfr_count']['parent'][prid] = [tweet['tweet']['reply_count'],tweet['tweet']['favorite_count'],tweet['tweet']['retweet_count']]

	### if tweet is a reply
	if tweet['tweet']['in_reply_to_status_id'] != None:
		prid = int(tweet['user']['id'])
		if prid not in user_id_key['screen_name'].keys():
			user_id_key['screen_name'][prid] = ''
		if prid not in user_id_key['verified'].keys():
			user_id_key['verified'][prid] = ''
		if prid not in user_id_key['location'].keys():
			user_id_key['location'][prid] = ''
		if prid not in user_id_key['created_at'].keys():
			user_id_key['created_at'][prid] = ''
		if prid not in user_id_key['followers_count'].keys():
			user_id_key['followers_count'][prid] = []
		if prid not in user_id_key['friends_count'].keys():
			user_id_key['friends_count'][prid] = []
		if prid not in user_id_key['tweet_id'].keys():
			user_id_key['tweet_id'][prid] = []
		user_id_key['screen_name'][prid] = tweet['user']['screen_name']
		user_id_key['verified'][prid] = format_boolean(tweet['user']['verified'])
		user_id_key['location'][prid] = tweet['user']['location']
		user_id_key['created_at'][prid] = format_date(tweet['user']['created_at'])
		user_id_key['followers_count'][prid].append(tweet['user']['followers_count'])
		user_id_key['friends_count'][prid].append(tweet['user']['friends_count'])
		user_id_key['tweet_id'][prid].append(tweet['tweet']['id'])

		### tweet information
		## parent tweet id
		prid = int(tweet['tweet']['in_reply_to_status_id'])
		## reply tweet id
		rpid = int(tweet['tweet']['id'])
		# user id
		tweet_id_key['user_id']['parent'][prid] = int(tweet['tweet']['in_reply_to_user_id'])
		if prid not in tweet_id_key['user_id']['reply'].keys():
			tweet_id_key['user_id']['reply'][prid] = []
		tweet_id_key['user_id']['reply'][prid].append(int(tweet['user']['id']))
		# tweet id
		tweet_id_key['tweet_id']['parent'][prid] = prid
		if prid not in tweet_id_key['tweet_id']['reply'].keys():
			tweet_id_key['tweet_id']['reply'][prid] = []
		tweet_id_key['tweet_id']['reply'][prid].append(rpid)
		tweet_id_key['tweet_id']['reply-reverse'][rpid] = prid
		# text
		if prid not in tweet_id_key['text']['parent'].keys():
			tweet_id_key['text']['parent'][prid] = []
		tweet_id_key['text']['parent'][prid] = tweet_id_key['text']['parent'][prid]
		if prid not in tweet_id_key['text']['reply'].keys():
			tweet_id_key['text']['reply'][prid] = []
		tweet_id_key['text']['reply'][prid].append(format_text(tweet['tweet']['text']))
		# time
		if prid not in tweet_id_key['time']['parent'].keys():
			tweet_id_key['time']['parent'][prid] = []
		tweet_id_key['time']['parent'][prid] = tweet_id_key['time']['parent'][prid]
		if prid not in tweet_id_key['time']['reply'].keys():
			tweet_id_key['time']['reply'][prid] = []
		tweet_id_key['time']['reply'][prid].append(format_date(tweet['tweet']['created_at']))
		# hashtags
		if prid not in tweet_id_key['hashtags']['parent'].keys():
			tweet_id_key['hashtags']['parent'][prid] = []
		tweet_id_key['hashtags']['parent'][prid] = tweet_id_key['hashtags']['parent'][prid]
		if prid not in tweet_id_key['hashtags']['reply'].keys():
			tweet_id_key['hashtags']['reply'][prid] = []
		tweet_id_key['hashtags']['reply'][prid].append(tweet['tweet']['hashtags'])
		# user_mentions
		if prid not in tweet_id_key['user_mentions']['parent'].keys():
			tweet_id_key['user_mentions']['parent'][prid] = []
		tweet_id_key['user_mentions']['parent'][prid] = tweet_id_key['user_mentions']['parent'][prid]
		if prid not in tweet_id_key['user_mentions']['reply'].keys():
			tweet_id_key['user_mentions']['reply'][prid] = []
		tweet_id_key['user_mentions']['reply'][prid].append(tweet['tweet']['user_mentions'])
		# language
		if prid not in tweet_id_key['language']['parent'].keys():
			tweet_id_key['language']['parent'][prid] = []
		tweet_id_key['language']['parent'][prid] = tweet_id_key['language']['parent'][prid]
		if prid not in tweet_id_key['language']['reply'].keys():
			tweet_id_key['language']['reply'][prid] = []
		tweet_id_key['language']['reply'][prid].append(tweet['tweet']['lang'])
		# reply-favorite-retweet (rfr) counts
		if prid not in tweet_id_key['rfr_count']['parent'].keys():
			tweet_id_key['rfr_count']['parent'][prid] = []
		tweet_id_key['rfr_count']['parent'][prid] = tweet_id_key['rfr_count']['parent'][prid]
		if prid not in tweet_id_key['rfr_count']['reply'].keys():
			tweet_id_key['rfr_count']['reply'][prid] = []
		tweet_id_key['rfr_count']['reply'][prid].append(tweet_id_key['rfr_count']['parent'][prid])

	### if tweet is a retweet
	if tweet['retweeted_status'] != None:
		### user information
		prid = int(tweet['retweeted_status']['user']['id'])
		if prid not in user_id_key['screen_name'].keys():
			user_id_key['screen_name'][prid] = ''
		if prid not in user_id_key['verified'].keys():
			user_id_key['verified'][prid] = ''
		if prid not in user_id_key['location'].keys():
			user_id_key['location'][prid] = ''
		if prid not in user_id_key['created_at'].keys():
			user_id_key['created_at'][prid] = ''
		if prid not in user_id_key['followers_count'].keys():
			user_id_key['followers_count'][prid] = []
		if prid not in user_id_key['friends_count'].keys():
			user_id_key['friends_count'][prid] = []
		if prid not in user_id_key['tweet_id'].keys():
			user_id_key['tweet_id'][prid] = []
		user_id_key['screen_name'][prid] = tweet['retweeted_status']['user']['screen_name']
		user_id_key['verified'][prid] = format_boolean(tweet['retweeted_status']['user']['verified'])
		user_id_key['location'][prid] = tweet['retweeted_status']['user']['location']
		user_id_key['created_at'][prid] = format_date(tweet['retweeted_status']['user']['created_at'])
		user_id_key['followers_count'][prid].append(tweet['retweeted_status']['user']['followers_count'])
		user_id_key['friends_count'][prid].append(tweet['retweeted_status']['user']['friends_count'])
		user_id_key['tweet_id'][prid].append(tweet['tweet']['id'])

		### tweet information
		## parent retweet id
		prid = int(tweet['retweeted_status']['tweet']['id'])
		## retweet retweet id
		rtid = int(tweet['tweet']['id'])
		# user id
		tweet_id_key['user_id']['parent'][prid] = tweet['retweeted_status']['user']['id']
		if prid not in tweet_id_key['user_id']['retweet'].keys():
			tweet_id_key['user_id']['retweet'][prid] = []
		tweet_id_key['user_id']['retweet'][prid].append(tweet['user']['id'])
		# tweet id
		tweet_id_key['tweet_id']['parent'][prid] = prid
		if prid not in tweet_id_key['tweet_id']['retweet'].keys():
			tweet_id_key['tweet_id']['retweet'][prid] = []
		tweet_id_key['tweet_id']['retweet'][prid].append(rtid)
		# text
		tweet_id_key['text']['parent'][prid] = format_text(tweet['retweeted_status']['tweet']['text'])
		# time
		tweet_id_key['time']['parent'][prid] = format_date(tweet['retweeted_status']['tweet']['created_at'])
		if prid not in tweet_id_key['time']['retweet'].keys():
			tweet_id_key['time']['retweet'][prid] = []
		tweet_id_key['time']['retweet'][prid].append(format_date(tweet['tweet']['created_at']))
		# hashtags
		tweet_id_key['hashtags']['parent'][prid] = tweet['retweeted_status']['tweet']['hashtags']
		# user-mentions
		tweet_id_key['user_mentions']['parent'][prid] = tweet['retweeted_status']['tweet']['user_mentions']
		# language
		tweet_id_key['language']['parent'][prid] = tweet['retweeted_status']['tweet']['lang']
		# reply-favorite-retweet (rfr) counts
		if prid not in tweet_id_key['rfr_count']['parent'].keys():
			tweet_id_key['rfr_count']['parent'][prid] = []
		tweet_id_key['rfr_count']['parent'][prid] = [tweet['retweeted_status']['tweet']['reply_count'],tweet['retweeted_status']['tweet']['favorite_count'],tweet['retweeted_status']['tweet']['retweet_count']]
		if prid not in tweet_id_key['rfr_count']['retweet'].keys():
			tweet_id_key['rfr_count']['retweet'][prid] = []
		tweet_id_key['rfr_count']['retweet'][prid].append([tweet['retweeted_status']['tweet']['reply_count'],tweet['retweeted_status']['tweet']['favorite_count'],tweet['retweeted_status']['tweet']['retweet_count']])

		if tweet['retweeted_status']['tweet']['in_reply_to_status_id'] != None:
			prid = int(tweet['retweeted_status']['user']['id'])
			if prid not in user_id_key['screen_name'].keys():
				user_id_key['screen_name'][prid] = ''
			if prid not in user_id_key['verified'].keys():
				user_id_key['verified'][prid] = ''
			if prid not in user_id_key['location'].keys():
				user_id_key['location'][prid] = ''
			if prid not in user_id_key['created_at'].keys():
				user_id_key['created_at'][prid] = ''
			if prid not in user_id_key['followers_count'].keys():
				user_id_key['followers_count'][prid] = []
			if prid not in user_id_key['friends_count'].keys():
				user_id_key['friends_count'][prid] = []
			if prid not in user_id_key['tweet_id'].keys():
				user_id_key['tweet_id'][prid] = []
			user_id_key['screen_name'][prid] = tweet['retweeted_status']['user']['screen_name']
			user_id_key['verified'][prid] = format_boolean(tweet['retweeted_status']['user']['verified'])
			user_id_key['location'][prid] = tweet['retweeted_status']['user']['location']
			user_id_key['created_at'][prid] = format_date(tweet['retweeted_status']['user']['created_at'])
			user_id_key['followers_count'][prid].append(tweet['retweeted_status']['user']['followers_count'])
			user_id_key['friends_count'][prid].append(tweet['retweeted_status']['user']['friends_count'])
			user_id_key['tweet_id'][prid].append(tweet['retweeted_status']['tweet']['id'])

			### tweet information
			## parent retweet id
			prid = int(tweet['retweeted_status']['tweet']['in_reply_to_status_id'])
			## reply tweet id
			rpid = int(tweet['retweeted_status']['tweet']['id'])
			# user id
			tweet_id_key['user_id']['parent'][prid] = int(tweet['retweeted_status']['tweet']['in_reply_to_user_id'])
			if prid not in tweet_id_key['user_id']['reply'].keys():
				tweet_id_key['user_id']['reply'][prid] = []
			tweet_id_key['user_id']['reply'][prid].append(tweet['retweeted_status']['user']['id'])
			# tweet id
			tweet_id_key['tweet_id']['parent'][prid] = int(tweet['retweeted_status']['tweet']['in_reply_to_status_id'])
			if prid not in tweet_id_key['tweet_id']['reply'].keys():
				tweet_id_key['tweet_id']['reply'][prid] = []
			tweet_id_key['tweet_id']['reply'][prid].append(rpid)
			tweet_id_key['tweet_id']['reply-reverse'][rpid] = prid
			# text
			if prid not in tweet_id_key['text']['parent'].keys():
				tweet_id_key['text']['parent'][prid] = []
			tweet_id_key['text']['parent'][prid] = tweet_id_key['text']['parent'][prid]
			if prid not in tweet_id_key['text']['reply'].keys():
				tweet_id_key['text']['reply'][prid] = []
			tweet_id_key['text']['reply'][prid].append(format_text(tweet['tweet']['text']))
			# time
			if prid not in tweet_id_key['time']['parent'].keys():
				tweet_id_key['time']['parent'][prid] = []
			tweet_id_key['time']['parent'][prid] = tweet_id_key['time']['parent'][prid]
			if prid not in tweet_id_key['time']['reply'].keys():
				tweet_id_key['time']['reply'][prid] = []
			tweet_id_key['time']['reply'][prid].append(format_date(tweet['tweet']['created_at']))
			# hashtags
			if prid not in tweet_id_key['hashtags']['parent'].keys():
				tweet_id_key['hashtags']['parent'][prid] = []
			tweet_id_key['hashtags']['parent'][prid] = tweet_id_key['hashtags']['parent'][prid]
			if prid not in tweet_id_key['hashtags']['reply'].keys():
				tweet_id_key['hashtags']['reply'][prid] = []
			tweet_id_key['hashtags']['reply'][prid].append(tweet['tweet']['hashtags'])
			# user_mentions
			if prid not in tweet_id_key['user_mentions']['parent'].keys():
				tweet_id_key['user_mentions']['parent'][prid] = []
			tweet_id_key['user_mentions']['parent'][prid] = tweet_id_key['user_mentions']['parent'][prid]
			if prid not in tweet_id_key['user_mentions']['reply'].keys():
				tweet_id_key['user_mentions']['reply'][prid] = []
			tweet_id_key['user_mentions']['reply'][prid].append(tweet['tweet']['user_mentions'])
			# language
			if prid not in tweet_id_key['language']['parent'].keys():
				tweet_id_key['language']['parent'][prid] = []
			tweet_id_key['language']['parent'][prid] = tweet_id_key['language']['parent'][prid]
			if prid not in tweet_id_key['language']['reply'].keys():
				tweet_id_key['language']['reply'][prid] = []
			tweet_id_key['language']['reply'][prid].append(tweet['tweet']['lang'])
			# reply-favorite-retweet (rfr) counts
			if prid not in tweet_id_key['rfr_count']['parent'].keys():
				tweet_id_key['rfr_count']['parent'][prid] = []
			tweet_id_key['rfr_count']['parent'][prid] = tweet_id_key['rfr_count']['parent'][prid]
			if prid not in tweet_id_key['rfr_count']['reply'].keys():
				tweet_id_key['rfr_count']['reply'][prid] = []
			tweet_id_key['rfr_count']['reply'][prid].append(tweet_id_key['rfr_count']['parent'][prid])

# process filtered tweets
def process_filteredTweets(STDVID,dates):
	start = time.time()
	# get dates information
	dates_label = dates[0]+'-'+dates[len(dates)-1]
	months = []
	for d in dates:
		month = d[0:2]
		year = d[len(d)-4:len(d)]
		months.append(month+year)
	months = np.unique(months)

	# tar.gz filenames
	month_filenames = []
	day_filenames = []
	for m in months:
		month_filenames.append(STDVID+'-filtered/keywordStream'+STDVID+'Tweets_'+m+'_filtered.json.tar.gz')
		day_list = []
		for d in dates:
			day_filename = 'keywordStream'+STDVID+'Tweets_'+d+'_filtered.json.tar.gz'
			day_list.append(day_filename)
		day_filenames.append(day_list)

	# main process loop - paralleled
	for i, month in enumerate(month_filenames):
		file0_tar = tarfile.open(month)
		for j, day in enumerate(day_filenames[i]):
			file0_tar.extract(file0_tar.getmember(day))
			file1_tar = tarfile.open(day)
			file2_json = file1_tar.extractfile(day.replace('.tar.gz',''))
			print('Processing '+day+'...')
			Parallel(n_jobs=num_cores,backend='threading')(delayed(organizer)(k) for k in file2_json)
			file1_tar.close()
			file2_json.close()

			## delete json.tar.gz file
			os.remove(day_filenames[i][j])

		file0_tar.close()

	out = {}
	out['user'] = user_id_key
	out['tweet'] = tweet_id_key

	directory_0 = STDVID+'-processed/'
	try:
			os.makedirs(directory_0)
	except FileExistsError:
		pass

	# save data and compress
	np.save(directory_0+'keywordStream'+STDVID+'Tweets_'+dates_label+'_processed',out)
	with tarfile.open(directory_0+'keywordStream'+STDVID+'Tweets_'+dates_label+'_processed.npy.tar.gz', "w:gz") as tar:
		source_dir = directory_0+'keywordStream'+STDVID+'Tweets_'+dates_label+'_processed.npy'
		tar.add(source_dir, arcname=os.path.basename(source_dir))
	os.remove(directory_0+'keywordStream'+STDVID+'Tweets_'+dates_label+'_processed.npy')
	del out
	print('Finished processing!')
	end = time.time()
	print('Computing time: '+str(round(end-start,2))+' seconds.')
	print()

### main run
process_filteredTweets(STDVID,dates)
sys.exit()
