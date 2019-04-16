#!/usr/bin/env bash

path_twitterFiles="/home/[username]/twitter-files"

STDVID=A
stopDay=14
stopMonth=08
stopYear=2018
stopHour=18
stopMinute=14
tweetsPerLoop=4000

./loop.streamTweets.sh ${path_twitterFiles} ${STDVID} ${stopDay} ${stopMonth} ${stopYear} ${stopHour} ${stopMinute} ${tweetsPerLoop}