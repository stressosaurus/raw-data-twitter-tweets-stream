### Twitter Data Tools
## Alex John Quijano
## Created: 8/15/2018
## Updated: 8/23/2018

import os
import re
import sys
import math
import nltk
import errno
import tarfile
import unidecode
import numpy as np
import pandas as pd
import subprocess as sb

def get_id_sets(data):
	parent = list(data['tweet']['tweet_id']['parent'].keys())
	retweet = list(data['tweet']['tweet_id']['retweet'].keys())
	reply = list(data['tweet']['tweet_id']['reply'].keys())
	replies = []
	for i in reply:
		replies.extend(data['tweet']['tweet_id']['reply'][i])
	replies = np.unique(replies)
	normal = list(set(parent) - set(retweet) - set(reply) - set(replies))
	sets_ids = [retweet,reply,replies,normal]
	sets_label = ['retweet','reply','replies','normal']
	tweet_id_sets = {}
	code = {}
	for i, j in enumerate(sets_label):
		tweet_id_sets[j] = sets_ids[i]
		for k in sets_ids[i]:
			if k not in code.keys():
				code[k] = np.zeros(len(sets_label),dtype=int)
				code[k][i] = 1
			else:
				code[k][i] = 1
	tweet_id_sets['set-code'] = code
	tweet_id_sets['set-label'] = sets_label
	return tweet_id_sets

def read(STDVID,date_range):
	try:
		directory = '/twitter-tweets/'+STDVID+'-processed/'
		filename = 'keywordStream'+STDVID+'Tweets_'+date_range+'_processed.npy.tar.gz'
		filename_tar = directory+filename
		filename_tar_directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+filename_tar
		file0_tar = tarfile.open(filename_tar_directory)
		file0_tar.extract(filename.replace('.tar.gz',''))
		out = np.load(filename.replace('.tar.gz','')).item()
		os.remove(filename.replace('.tar.gz',''))
		file0_tar.close()
	except FileNotFoundError:
		try:
			directory = '/raw-data/twitter-tweets/'+STDVID+'-processed/'
			filename = 'keywordStream'+STDVID+'Tweets_'+date_range+'_processed.npy.tar.gz'
			filename_tar = directory+filename
			filename_tar_directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+filename_tar
			file0_tar = tarfile.open(filename_tar_directory)
			file0_tar.extract(filename.replace('.tar.gz',''))
			out = np.load(filename.replace('.tar.gz','')).item()
			os.remove(filename.replace('.tar.gz',''))
			file0_tar.close()
		except:
			print('Error: The '+STDVID+'-processed directory can not be found or the dataset does not exist anywhere.')
			print('Please refer to the README.md file.')
			try:
				sys.exit()
			except SystemExit:
				sys.exit
	# tweet id sets
	tweet_id_sets = get_id_sets(out)
	return out, tweet_id_sets

def tweet_id_categories(table,dtype=str):
	tweet_ids = {}
	if dtype != str:
		tweet_ids['retweets'] = table.index[table['RTT'] == 1].tolist()
		tweet_ids['replied'] = table.index[table['RPT'] == 1].tolist()
		tweet_ids['replies'] = table.index[table['TRP'] == 1].tolist()
		tweet_ids['normal'] = table.index[table['NRT'] == 1].tolist()
	else:
		tweet_ids['retweets'] = table.index[table['RTT'] == '1'].tolist()
		tweet_ids['replied'] = table.index[table['RPT'] == '1'].tolist()
		tweet_ids['replies'] = table.index[table['TRP'] == '1'].tolist()
		tweet_ids['normal'] = table.index[table['NRT'] == '1'].tolist()
	return tweet_ids

def read_csv(stream,date_range,dtype=str,subset=False,which_set='subset',which_set_params=[]):
	out_T = None
	out_U = None
	if subset == False:
		try:
			directory_1 = stream+'-tabulated/'
			pathway_T = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'_tabulated-tweet.csv.gz'
			pathway_U = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'_tabulated-user.csv.gz'
			out_T = pd.read_csv(pathway_T,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
			out_U = pd.read_csv(pathway_U,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
		except FileNotFoundError:
			try:
				directory_1 = '/raw-data/twitter-tweets/'+stream+'-tabulated/'
				pathway_T = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'_tabulated-tweet.csv.gz'
				pathway_U = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'_tabulated-user.csv.gz'
				out_T = pd.read_csv(pathway_T,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
				out_U = pd.read_csv(pathway_U,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
			except FileNotFoundError:
				directory_1 = '/twitter-tweets/'+stream+'-tabulated/'
				pathway_T = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'_tabulated-tweet.csv.gz'
				pathway_U = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'_tabulated-user.csv.gz'
				out_T = pd.read_csv(pathway_T,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
				out_U = pd.read_csv(pathway_U,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
	elif subset == True:
		try:
			directory_1 = stream+'-'+which_set+'/'
			pathway_T = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'-subset-'+which_set_params[0]+'-'+which_set_params[1]+'_tabulated-tweet.csv.gz'
			pathway_U = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'-subset-'+which_set_params[0]+'-'+which_set_params[1]+'_tabulated-user.csv.gz'
			out_T = pd.read_csv(pathway_T,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
			out_U = pd.read_csv(pathway_U,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
		except FileNotFoundError:
			try:
				directory_1 = '/raw-data/twitter-tweets/'+stream+'-'+which_set+'/'
				pathway_T = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'-subset-'+which_set_params[0]+'-'+which_set_params[1]+'_tabulated-tweet.csv.gz'
				pathway_U = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'-subset-'+which_set_params[0]+'-'+which_set_params[1]+'_tabulated-user.csv.gz'
				out_T = pd.read_csv(pathway_T,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
				out_U = pd.read_csv(pathway_U,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
			except FileNotFoundError:
				directory_1 = '/twitter-tweets/'+stream+'-'+which_set+'/'
				pathway_T = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'-subset-'+which_set_params[0]+'-'+which_set_params[1]+'_tabulated-tweet.csv.gz'
				pathway_U = os.path.abspath(os.path.join(os.getcwd(), os.pardir))+directory_1+stream+'-processed-'+date_range+'-subset-'+which_set_params[0]+'-'+which_set_params[1]+'_tabulated-user.csv.gz'
				out_T = pd.read_csv(pathway_T,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
				out_U = pd.read_csv(pathway_U,compression='gzip',sep=',',index_col=0,header=0,dtype=str)
	return out_T, out_U

def merge_tables(tables):
	merged_tables = pd.concat(tables)
	tweet_ids = tweet_id_categories(merged_tables)
	return merged_tables, tweet_ids

def tabler(i,table,table_UID,data,tweet_ids):
	try:
		tester = table[i]
	except KeyError:
		table[i] = []

	# table columns:
	#   1.  TID    - Tweet ID (always unique)
	#   2.  RTT    - Retweeted Tweets (1-True or 0-False)
	#   3.  RPT    - Replied Tweets (1-True or 0-False)
	#   4.  TRP    - Tweet Replies (1-True or 0-False)
	#   5.  NRT    - Normal Tweets (1-True or 0-False)
	#   6.  PTID   - Parent Tweet ID (replies only)
	#   7.	UID    - User ID
	#   8.	USN    - User screen name
	#   9.	TM     - Time created
	#   10.	RPC    - Replied Counts
	#   11. FVC    - Favorited Counts
	#   12. RTC    - Retweeted Counts
	#   13.	L      - language
	#   14.	TXT    - Tweet Text
	#   15.	HTGS   - Hashtags (sep=',')
	#   16.	UMS    - user mentions (sep=',')
	#   17.	TMRT   - Time if retweeted (sep=',')
	#   18.	RPCRT  - Replied Counts if retweeted (sep=',')
	#   19.	FVCRT  - Favorited Counts if retweeted (sep=',')
	#   20.	RTCRT  - Retweeted Counts if retweeted (sep=',')
	#	21.	UIDRT  - User ID if retweeted (sep=',')
	#	22.	USNRT  - User screen name if retweeted (sep=',')

	# table_UID columns
	#	1. UID  - User screen screen_name
	#	2. USN  - User screen screen_name
	#	3. UT   - Time user created
	#	4. UNFL - User number of followers (sep=',')

	# missing data
	missing_data_char = '*'

	# RTT, RPT, TRP, NRT
	code = tweet_ids['set-code'][i]
	table[i].append(code[0]) # retweet
	table[i].append(code[1]) # reply
	table[i].append(code[2]) # replies
	table[i].append(code[3]) # normal
	# PTID
	try:
		parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
		table[i].append(str(parent_id))
	except:
		table[i].append(missing_data_char)
	# UID
	if code[2] != 1:
		user_id = data['tweet']['user_id']['parent'][i]
		table[i].append(str(user_id))
		try:
			tester_UID = table_UID[user_id]
		except:
			table_UID[user_id] = [missing_data_char]*3
	else:
		if code[0] == 1:
			user_id = data['tweet']['user_id']['parent'][i]
			table[i].append(str(user_id))
			try:
				tester_UID = table_UID[user_id]
			except:
				table_UID[user_id] = [missing_data_char]*3
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				user_id = data['tweet']['user_id']['reply'][parent_id][index]
				table[i].append(str(user_id))
				try:
					tester_UID = table_UID[user_id]
				except:
					table_UID[user_id] = [missing_data_char]*3
			except:
				table[i].append(missing_data_char)
	# USN
	if code[2] != 1:
		user_id = data['tweet']['user_id']['parent'][i]
		try:
			screen_name = data['user']['screen_name'][user_id]
			table[i].append(str(screen_name))
			table_UID[user_id][0] = screen_name
		except:
			table[i].append(missing_data_char)
	else:
		if code[0] == 1:
			user_id = data['tweet']['user_id']['parent'][i]
			try:
				screen_name = data['user']['screen_name'][user_id]
				table[i].append(str(screen_name))
				table_UID[user_id][0] = screen_name
			except:
				table[i].append(missing_data_char)
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				user_id = data['tweet']['user_id']['reply'][parent_id][index]
				try:
					screen_name = data['user']['screen_name'][user_id]
					table[i].append(str(screen_name))
					table_UID[user_id][0] = screen_name
				except:
					table[i].append(missing_data_char)
			except:
				table[i].append(missing_data_char)
	# TM
	if code[2] != 1:
		times = data['tweet']['time']['parent'][i]
		if times == [] or times == None:
			table[i].append(missing_data_char)
		else:
			table[i].append(str(times))
	else:
		if code[0] == 1:
			times = data['tweet']['time']['parent'][i]
			if times == [] or times == None:
				table[i].append(missing_data_char)
			else:
				table[i].append(str(times))
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				times = data['tweet']['time']['reply'][parent_id][index]
				if times == [] or times == None:
					table[i].append(missing_data_char)
				else:
					table[i].append(str(times))
			except:
				table[i].append(missing_data_char)
	# RPC, FVC, RTC
	if code[2] != 1:
		rfr = data['tweet']['rfr_count']['parent'][i]
		if rfr == [] or rfr == None:
			table[i].append(missing_data_char)
			table[i].append(missing_data_char)
			table[i].append(missing_data_char)
		else:
			table[i].append(str(rfr[0]))
			table[i].append(str(rfr[1]))
			table[i].append(str(rfr[2]))
	else:
		if code[0] == 1:
			rfr = data['tweet']['rfr_count']['parent'][i]
			if rfr == [] or rfr == None:
				table[i].append(missing_data_char)
				table[i].append(missing_data_char)
				table[i].append(missing_data_char)
			else:
				table[i].append(str(rfr[0]))
				table[i].append(str(rfr[1]))
				table[i].append(str(rfr[2]))
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				rfr = data['tweet']['rfr_count']['reply'][parent_id][index]
				if rfr == [] or rfr == None:
					table[i].append(missing_data_char)
					table[i].append(missing_data_char)
					table[i].append(missing_data_char)
				else:
					table[i].append(str(rfr[0]))
					table[i].append(str(rfr[1]))
					table[i].append(str(rfr[2]))
			except:
				table[i].append(missing_data_char)
				table[i].append(missing_data_char)
				table[i].append(missing_data_char)
	# L
	if code[2] != 1:
		l = data['tweet']['language']['parent'][i]
		if l != []:
			table[i].append(l)
		else:
			table[i].append(missing_data_char)
	else:
		if code[0] == 1:
			l = data['tweet']['language']['parent'][i]
			if l != []:
				table[i].append(l)
			else:
				table[i].append(missing_data_char)
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				l = data['tweet']['language']['reply'][parent_id][index]
				if l != []:
					table[i].append(l)
				else:
					table[i].append(missing_data_char)
			except:
				table[i].append(missing_data_char)
	# TXT
	if code[2] != 1:
		text = data['tweet']['text']['parent'][i]
		if text == [] or text == None:
			table[i].append(missing_data_char)
		else:
			table[i].append(re.sub(r'[\t\n\s]+',' ',text))
		del text
	else:
		if code[0] == 1:
			text = data['tweet']['text']['parent'][i]
			if text == [] or text == None:
				table[i].append(missing_data_char)
			else:
				table[i].append(re.sub(r'[\t\n\s]+',' ',text))
			del text
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				text = data['tweet']['text']['reply'][parent_id][index]
				if text == [] or text == None:
					table[i].append(missing_data_char)
				else:
					table[i].append(re.sub(r'[\t\n\s]+',' ',text))
				del parent_id, index
			except:
				table[i].append(missing_data_char)
	# HTGS
	if code[2] != 1:
		hashtags = data['tweet']['hashtags']['parent'][i]
		if hashtags == [] or hashtags == None:
			table[i].append(missing_data_char)
		elif ','.join(hashtags) == 'null' or ','.join(hashtags) == 'nan':
			table[i].append(missing_data_char)
		else:
			table[i].append(','.join(hashtags))
	else:
		if code[0] == 1:
			hashtags = data['tweet']['hashtags']['parent'][i]
			if hashtags == [] or hashtags == None:
				table[i].append(missing_data_char)
			elif ','.join(hashtags) == 'null' or ','.join(hashtags) == 'nan':
				table[i].append(missing_data_char)
			else:
				table[i].append(','.join(hashtags))
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				hashtags = data['tweet']['hashtags']['reply'][parent_id][index]
				if hashtags == [] or hashtags == None:
					table[i].append(missing_data_char)
				elif ','.join(hashtags) == 'null' or ','.join(hashtags) == 'nan':
					table[i].append(missing_data_char)
				else:
					table[i].append(','.join(hashtags))
			except:
				table[i].append(missing_data_char)
	# UMS
	if code[2] != 1:
		user_mentions = data['tweet']['user_mentions']['parent'][i]
		if user_mentions == [] or user_mentions == None:
			table[i].append(missing_data_char)
		elif ','.join(user_mentions) == 'null' or ','.join(user_mentions) == 'nan':
			table[i].append(missing_data_char)
		else:
			table[i].append(','.join(user_mentions))
	else:
		if code[0] == 1:
			user_mentions = data['tweet']['user_mentions']['parent'][i]
			if user_mentions == [] or user_mentions == None:
				table[i].append(missing_data_char)
			elif ','.join(user_mentions) == 'null' or ','.join(user_mentions) == 'nan':
				table[i].append(missing_data_char)
			else:
				table[i].append(','.join(user_mentions))
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				user_id = data['tweet']['user_id']['reply'][parent_id][index]
				user_mentions = data['tweet']['user_mentions']['reply'][parent_id][index]
				if user_mentions == [] or user_mentions == None:
					table[i].append(missing_data_char)
				elif ','.join(user_mentions) == 'null' or ','.join(user_mentions) == 'nan':
					table[i].append(missing_data_char)
				else:
					table[i].append(','.join(user_mentions))
			except:
				table[i].append(missing_data_char)
	# TMRT
	try:
		times = data['tweet']['time']['retweet'][i]
		if times == [] or times == None:
			table[i].append(missing_data_char)
		else:
			table[i].append(str(','.join(times)))
	except:
		table[i].append(missing_data_char)
	# RPCRT, FVCRT, RTCRT
	try:
		rfr_vect = data['tweet']['rfr_count']['retweet'][i]
		rfr_0 = []
		rfr_1 = []
		rfr_2 = []
		for rfr_i in rfr_vect:
			rfr_0.append(str(rfr_i[0]))
			rfr_1.append(str(rfr_i[1]))
			rfr_2.append(str(rfr_i[2]))
		table[i].append(str(','.join(rfr_0)))
		table[i].append(str(','.join(rfr_1)))
		table[i].append(str(','.join(rfr_2)))
	except:
		table[i].append(missing_data_char)
		table[i].append(missing_data_char)
		table[i].append(missing_data_char)
	# UIDRT
	try:
		user_id_rt = [str(uid) for uid in data['tweet']['user_id']['retweet'][i]]
		table[i].append(str(','.join(user_id_rt)))
		for uidrt in user_id_rt:
			try:
				tester_UID = table_UID[uidrt]
			except:
				table_UID[uidrt] = [missing_data_char]*3
	except:
		table[i].append(missing_data_char)
	# USNRT
	try:
		user_id_rt = data['tweet']['user_id']['retweet'][i]
		screen_name_rt = []
		for uidrt in user_id_rt:
			try:
				screen_name = data['user']['screen_name'][uidrt]
				screen_name_rt.append(screen_name)
				table_UID[uidrt][0] = screen_name
			except:
				screen_name_rt.append(missing_data_char)
		table[i].append(str(','.join(screen_name_rt)))
	except:
		table[i].append(missing_data_char)
	# UT and UNFL
	if code[2] != 1:
		user_id = data['tweet']['user_id']['parent'][i]
		try:
			user_time_created = data['user']['created_at'][user_id]
			flc = data['user']['followers_count'][user_id]
			table_UID[user_id][1] = user_time_created
			table_UID[user_id][2] = str(','.join([str(k) for k in flc]))
		except:
			pass
	else:
		if code[0] == 1:
			user_id = data['tweet']['user_id']['parent'][i]
			try:
				user_time_created = data['user']['created_at'][user_id]
				flc = data['user']['followers_count'][user_id]
				table_UID[user_id][1] = user_time_created
				table_UID[user_id][2] = str(','.join([str(k) for k in flc]))
			except:
				pass
		else:
			try:
				parent_id = data['tweet']['tweet_id']['reply-reverse'][i]
				index = data['tweet']['tweet_id']['reply'][parent_id].index(i)
				user_id = data['tweet']['user_id']['reply'][parent_id][index]
				try:
					user_time_created = data['user']['created_at'][user_id]
					flc = data['user']['followers_count'][user_id]
					table_UID[user_id][1] = user_time_created
					table_UID[user_id][2] = str(','.join([str(k) for k in flc]))
				except:
					pass
			except:
				pass
	# UTRT and UNFLTRT
	try:
		user_id_rt = data['tweet']['user_id']['retweet'][i]
		for uidrt in user_id_rt:
			try:
				user_time_created = data['user']['created_at'][uidrt]
				flc = data['user']['followers_count'][uidrt]
				table_UID[uidrt][1] = user_time_created
				table_UID[uidrt][2] = str(','.join([str(k) for k in flc]))
				del user_time_created
			except:
				pass
	except:
		pass

def tabulate(data,tweet_ids):

	ids = list(tweet_ids['set-code'].keys())
	table = {}
	table_UID = {}
	for k in ids:
		tabler(k,table,table_UID,data,tweet_ids)

	# table
	table = pd.DataFrame(table)
	table = table.transpose()
	table.columns = ['RTT','RPT','TRP','NRT',
					 'PTID','UID','USN','TM',
					 'RPC','FVC','RTC','L',
					 'TXT','HTGS','UMS',
					 'TMRT','RPCRT','FVCRT','RTCRT',
					 'UIDRT','USNRT']

	# table UID
	table_UID = pd.DataFrame(table_UID)
	table_UID = table_UID.transpose()
	table_UID.columns = ['USN','UT','UNFL']
	drop_rows_UID = []
	for k in list(table_UID['USN'].keys()):
		row_vect = table_UID.loc[k,:]
		try:
			if ''.join(list(row_vect.values)) == '***':
				drop_rows_UID.append(k)
		except:
			pass
	table_UID_final = table_UID.drop(drop_rows_UID)

	del ids, drop_rows_UID
	return table, table_UID_final

def language_code(l):
	languages = {'ar':'Arabic','cs':'Czech','cy':'Welsh',
	             'da':'Danish', 'de':'German', 'es':'Spanish',
				 'et':'Estonian', 'eu':'Basque', 'fi':'Finnish',
				 'fr':'French', 'hi':'Hindi', 'hu':'Hungarian',
				 'in':'Indonesian', 'it':'Italian', 'iw':'Hebrew',
				 'ja':'Japanese', 'ko':'Korean', 'lv':'Latvian',
				 'ml':'Malayalam', 'nl':'Dutch', 'no':'Norwegian',
				 'pl':'Polish', 'pt':'Portugese', 'ro':'Romanian',
				 'sv':'Swedish', 'th':'Thai', 'tl':'Tagalog',
 			 	 'tr':'Turkish', 'und':'Undefined', 'ur':'Urdu',
				 'en':'English'}
	try:
		return languages[l]
	except:
		try:
			languages = {y:x for x,y in languages.iteritems()}
			return languages[l]
		except:
			return 'code-unr'

# stop words - get indices of ngrams with stop words
def stopWord_getIndex(strings,l):
	# defined stop words
	stop_words = []
	file = open('1gram-list/1gram-list-stop-word-'+l)
	for f in file:
		stop_words.append(f.replace('\n',''))
	index_out = []
	for i, j in enumerate(strings):
		if j in stop_words:
			index_out.append(i)
	return np.array(index_out)

# bins the tweets by hour (UTC)
def time_binner(T,by='hour'):
	month_code = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
	month_range = {1:[1,31],2:[1,28],3:[1,31],4:[1,30],5:[1,31],6:[1,30],7:[1,31],8:[1,31],9:[1,30],10:[1,31],11:[1,30],12:[1,31]}
	# time stamps and tweet ids
	TID_time = []
	TID_list = []
	TID_time_on = []
	for i in list(T['RTT'].keys()):
		if T['RTT'][i] == 1 or T['RTT'][i] == '1':
			time = T['TM'][i]
			if time != '*':
				TID_time.append(time)
				TID_list.append(i)
			timeRT = T['TMRT'][i].split(',')
			if timeRT != '*':
				count = len(timeRT)
				for j in range(0,count):
					if timeRT[j] != '*':
						TID_time.append(timeRT[j])
						TID_list.append(i)
						TID_time_on.append(timeRT[j])
		if T['NRT'][i] == 1 or T['NRT'][i] == '1':
			time = T['TM'][i]
			if time != '*':
				TID_time_on.append(time)

    # streamer-on bins
	split_time_stamps = [np.array(i.split(' ')) for i in TID_time_on]
	time_on = np.matrix(split_time_stamps)
	time_on_vect = {'Y':[],'M':[],'D':[],'H':[],'N':[],'S':[],'Z':[]}
	for i in range(0,time_on.shape[0]):
		for j in range(0,time_on.shape[1]):
			if j == 0:
				time_on_vect['M'].append(month_code[time_on[i,j]])
			elif j == 1:
				time_on_vect['D'].append(int(time_on[i,j]))
			elif j == 2:
				time_on_vect['Y'].append(int(time_on[i,j]))
			elif j == 3:
			    MT = time_on[i,j].split(':')
			    time_on_vect['H'].append(int(MT[0]))
			    time_on_vect['N'].append(int(MT[1]))
			    time_on_vect['S'].append(int(MT[2]))
			elif j == 4:
				time_on_vect['Z'].append(int(time_on[i,j].replace('+','')))
	time_on_vect = pd.DataFrame(time_on_vect).sort_values(by=['Y','M','D','H','N','S','Z'])
	Yu = np.unique(time_on_vect['Y'])
	Mu = np.unique(time_on_vect['M'])
	Du = []
	for m in Mu:
		day_index = time_on_vect.index[time_on_vect['M'] == m].tolist()
		days = time_on_vect['D'][day_index]
		days_min = days.min()
		days_max = days.max()
		Du.append(np.array([days_min,days_max]))
	Hu = np.unique(time_on_vect['H'])
	Nu = np.unique(time_on_vect['N'])

	bins_on = {}
	for i in range(Yu.min(),Yu.max()+1):
		for jj ,j in enumerate(range(Mu.min(),Mu.max()+1)):
			if jj == 0:
				minimum = Du[jj].min()
				maximum = month_range[j][1]+1
			elif jj == len(Du)-1:
				minimum = month_range[j][0]
				maximum = Du[jj].max()+1
			else:
				minimum = month_range[j][0]
				maximum = month_range[j][1]+1
			for k in range(minimum,maximum):
				if by == 'day':
					bins_on[i,j,k] = []
				elif by == 'hour':
					for l in range(0,24):
						bins_on[i,j,k,l] = []

	# populate bins
	for i, j in enumerate(TID_time):
		time = j.split(' ')
		for k, t in enumerate(time):
			if k == 0:
				month = month_code[t]
			elif k == 1:
				day = int(t)
			elif k == 2:
				year = int(t)
			elif k == 3:
				clock = t.split(':')
				hour = int(clock[0])
				minute = int(clock[1])
				second = int(clock[2])
			elif k == 4:
				time_zone = int(t.replace('+',''))
				tid = TID_list[i]
		if by == 'day':
			try:
				bins_on[year,month,day].append(tid)
			except:
				pass
		elif by == 'hour':
			try:
				bins_on[year,month,day,hour].append(tid)
			except:
				pass

	# time labels
	time_labels_main = []
	for t in bins_on.keys():
		m = str(t[1])
		if len(m) == 1:
			m = '0'+m
		if by == 'day' or by == 'hour':
			d = str(t[2])
			if len(d) == 1:
				d = '0'+d
			time_labels_main.append(m+'/'+d)
		elif by == 'hour':
			h = str(t[3])
			if len(h) == 1:
				h = '0'+h
			time_labels_main.append(m+'/'+d+' '+h+':00')
	hour_labels = (time_labels_main,range(0,len(time_labels_main)))
	day_labels_main = []
	for t in bins_on.keys():
	    m = str(t[1])
	    if len(m) == 1:
	        m = '0'+m
	    d = str(t[2])
	    if len(d) == 1:
	        d = '0'+d
	    day_labels_main.append(m+'/'+d)
	day_labels_main = list(np.unique(day_labels_main))
	day_labels_position = []
	for i in range(0,len(time_labels_main)+1):
	    if i%24 == 0:
	        day_labels_position.append(i+12)
	day_labels = (day_labels_main,day_labels_position)

	# count bin populations
	bins_on_count = {}
	for i in bins_on.keys():
		length = len(bins_on[i])
		if length != 0:
			bins_on_count[i] = length
		else:
			bins_on_count[i] = None

	return bins_on, bins_on_count, hour_labels, day_labels
