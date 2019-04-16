#!/usr/bin/env python3

### Live Twitter tweets streamer
## Alex John Quijano
## Created: 5/10/2017
## Updated: 8/16/2018

import os
import sys
from nltk.twitter import Query, Streamer, Twitter, TweetViewer, TweetWriter, credsfromfile
tw = Twitter()
oauth = credsfromfile()			
client = Query(**oauth)

# Search tweets using keywords
keywords = []
file = open('keywords-stream/keywords-stream'+sys.argv[1]+'.txt','r')
for line in file:
	keywords.append(line.replace('\n',''))
keys = ', '.join(keywords)

# Stream live Tweets
tw.tweets(keywords=keys,to_screen=False,stream=True,limit=int(sys.argv[2]))