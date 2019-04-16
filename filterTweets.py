#!/usr/bin/env python3

##!/home/[username]/miniconda3/bin/python

### Twitter Data filter
## Alex John Quijano
## Created: 8/15/2018
## Updated: 8/16/2018

import re
import sys
import json
import tarfile

print('Filtering '+sys.argv[1]+'...')
file0_tar = tarfile.open(sys.argv[1])
file0_tar_filename = sys.argv[1].replace('.tar.gz','')
file0_json = file0_tar.extractfile(file0_tar_filename)
file1_json = open(file0_tar_filename.replace('.json','')+'_filtered.json','w+')
for i in file0_json:
	tweet = json.loads(i)
	tweet_filtered = {}
	
	### Filtering - JSON
	
	# Create 'user' key - information about the user that created the tweet
	tweet_filtered['user'] = {
		'screen_name'			:	tweet['user']['screen_name'],
		'id'							:	tweet['user']['id'],
		'verified'				: tweet['user']['verified'],
		'location'				:	tweet['user']['location'],
		'time_zone'				: tweet['user']['time_zone'],
		'followers_count'	:	tweet['user']['followers_count'],
		'friends_count'		:	tweet['user']['friends_count'],
		'created_at'			: tweet['user']['created_at']}
	
	# Create 'tweet' key - information about the actual tweet
	tweet_filtered['tweet'] = {
		'text'										:	tweet['text'],
		'lang'										:	tweet['lang'],
		'created_at'							:	tweet['created_at'],
		'id'											:	tweet['id'],
		'reply_count'							: tweet['reply_count'],
		'favorite_count'					: tweet['favorite_count'],
		'retweet_count'						: tweet['retweet_count'],
		'in_reply_to_screen_name'	:	tweet['in_reply_to_screen_name'],
		'in_reply_to_user_id'			:	tweet['in_reply_to_user_id'],
		'in_reply_to_status_id'		:	tweet['in_reply_to_status_id']}		
	hashtags = tweet['entities']['hashtags']
	if len(hashtags) > 0:
		tweet_filtered['tweet']['hashtags'] = []
		for ht in hashtags:
			tweet_filtered['tweet']['hashtags'].append(ht['text'])
	else:
		tweet_filtered['tweet']['hashtags'] = None
	user_mentions = tweet['entities']['user_mentions']
	if len(user_mentions) > 0:
		tweet_filtered['tweet']['user_mentions'] = []
		for um in user_mentions:
			tweet_filtered['tweet']['user_mentions'].append(um['screen_name'])
	else:
		tweet_filtered['tweet']['user_mentions'] = None
		
	# Create 'retweeted_status' key (also meant for replies) - information whether the actual tweet is a reply or a retweeted tweet
	if tweet.get('retweeted_status'):
		tweet_filtered['retweeted_status'] = {
			'user'	:	{	'screen_name'							:	tweet['retweeted_status']['user']['screen_name'],
								 	'id'											: tweet['retweeted_status']['user']['id'],
								 	'verified'								: tweet['retweeted_status']['user']['verified'],
									'location'								:	tweet['retweeted_status']['user']['location'],
								 	'time_zone'								:	tweet['retweeted_status']['user']['time_zone'],
									'followers_count'					:	tweet['retweeted_status']['user']['followers_count'],
									'friends_count'						:	tweet['retweeted_status']['user']['friends_count'],
									'created_at'							: tweet['retweeted_status']['user']['created_at']},
			'tweet'	:	{	'text'										:	tweet['retweeted_status']['text'],
									'lang'										:	tweet['retweeted_status']['lang'],
									'created_at'							:	tweet['retweeted_status']['created_at'],
									'id'											:	tweet['retweeted_status']['id'],
								 	'reply_count'							: tweet['retweeted_status']['reply_count'],
								 	'favorite_count'					: tweet['retweeted_status']['favorite_count'],
									'retweet_count'						: tweet['retweeted_status']['retweet_count'],
									'in_reply_to_screen_name'	:	tweet['retweeted_status']['in_reply_to_screen_name'],
									'in_reply_to_user_id'			:	tweet['retweeted_status']['in_reply_to_user_id'],
									'in_reply_to_status_id'		:	tweet['retweeted_status']['in_reply_to_status_id']}}
		hashtags = tweet['retweeted_status']['entities']['hashtags']
		if len(hashtags) > 0:
			tweet_filtered['retweeted_status']['tweet']['hashtags'] = []
			for ht in hashtags:
				tweet_filtered['retweeted_status']['tweet']['hashtags'].append(ht['text'])
		else:
			tweet_filtered['retweeted_status']['tweet']['hashtags'] = None
		user_mentions = tweet['retweeted_status']['entities']['user_mentions']
		if len(user_mentions) > 0:
			tweet_filtered['retweeted_status']['tweet']['user_mentions'] = []
			for um in user_mentions:
				tweet_filtered['retweeted_status']['tweet']['user_mentions'].append(um['screen_name'])
		else:
			tweet_filtered['retweeted_status']['tweet']['user_mentions'] = None
	else:
		tweet_filtered['retweeted_status'] = None # if the actual tweet is just anormal tweet

	# Encode to JSON string
	tweet_filtered_JSON = json.dumps(tweet_filtered)
	file1_json.write(tweet_filtered_JSON + '\n')
file0_tar.close()
file0_json.close()
file1_json.close()