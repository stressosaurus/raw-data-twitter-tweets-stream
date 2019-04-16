#!/usr/bin/env bash

# Twitter credentials path
path_twitterFiles=$1
export TWITTER=$path_twitterFiles

STDVID=$2
f1="${STDVID}-raw"
f2="${STDVID}-filtered"
f3="temporary"

# Prepare folders
mkdir -p ${f1}
mkdir -p ${f2}
mkdir -p ${f3}

# Absolute path to temporary folder
path_twitterTemporary=$(realpath ${f3})

# Stream Tweets until set date and time
stopDay=$3
stopMonth=$4
stopYear=$5
stopHour=$6
stopMinute=$7
endMonth28=(02)
endMonth30=(04 06 09 11)
endMonth31=(01 03 05 07 08 10 12)

endOfStream=${stopYear}${stopMonth}${stopDay}${stopHour}${stopMinute}
dS=`date '+%Y%m%d%H%M'`

year=`date '+%Y'`
endOfYear=${year}1231
dY=`date '+%Y%m%d'`
while [ "$dY" -le "$endOfYear" ];
do

	## Break
	if [[ "$dS" -ge "$endOfStream" ]];
	then
		break;
	fi
	## Break

	month=`date '+%m'`
	if [[ ${endMonth28[*]} =~ ${month} ]];
	then
		endMonthDay=28
	fi
	if [[ ${endMonth30[*]} =~ ${month} ]];
	then
		endMonthDay=30
	fi
	if [[ ${endMonth31[*]} =~ ${month} ]];
	then
		endMonthDay=31
	fi
	endOfMonth=${month}${endMonthDay}
	dM=`date '+%m%d'`
	while [ "$dM" -le "$endOfMonth" ];
	do
	
		## Break
		if [[ "$dS" -ge "$endOfStream" ]];
		then
			break;
		fi
		## Break
	
		day=`date '+%d'`
		endOfDay=2330
		dH=`date '+%H%M'`
		while [ "$dH" -le "$endOfDay" ];
		do
		
			## Break
			if [[ "$dS" -ge "$endOfStream" ]];
			then
				break;
			fi
			## Break
		
			## Stream live tweets
			hour=`date '+%H'`
			echo -ne "Live twitter streaming $dH\n";
			./streamTweets.py ${STDVID} 10000 # Streamer
			mv $path_twitterFiles/tweets*.json $path_twitterTemporary
			dS=`date '+%Y%m%d%H%M'`
			dH=`date '+%H%M'`
			echo -ne "end $dH\n";
			
		done
		
		# Concatenate and compress per day
		cat ${f3}/tweets*.json >> keywordStream${STDVID}Tweets_${month}${day}${year}.json 2>/dev/null
		rm -f ${f3}/tweets*.json 2>/dev/null
		tar -czvf keywordStream${STDVID}Tweets_${month}${day}${year}.json.tar.gz keywordStream${STDVID}Tweets_${month}${day}${year}.json 2>/dev/null
		rm keywordStream${STDVID}Tweets_${month}${day}${year}.json 2>/dev/null

		# Filter tweets
		./filterTweets.py keywordStream${STDVID}Tweets_${month}${day}${year}.json.tar.gz 2>/dev/null # Filter
		tar -czvf keywordStream${STDVID}Tweets_${month}${day}${year}_filtered.json.tar.gz keywordStream${STDVID}Tweets_${month}${day}${year}_filtered.json 2>/dev/null
		rm keywordStream${STDVID}Tweets_${month}${day}${year}_filtered.json 2>/dev/null

		dM=`date '+%m%d'`
	done
	
	# compress per month	
	tar -czvf keywordStream${STDVID}Tweets_${month}${year}.json.tar.gz keywordStream${STDVID}Tweets_${month}*${year}.json.tar.gz 2>/dev/null
	mv keywordStream${STDVID}Tweets_${month}${year}.json.tar.gz ${f1} 2>/dev/null
	rm keywordStream${STDVID}Tweets_${month}*${year}.json.tar.gz 2>/dev/null
	tar -czvf keywordStream${STDVID}Tweets_${month}${year}_filtered.json.tar.gz keywordStream${STDVID}Tweets_${month}*${year}_filtered.json.tar.gz 2>/dev/null
	mv keywordStream${STDVID}Tweets_${month}${year}_filtered.json.tar.gz ${f2} 2>/dev/null
	rm keywordStream${STDVID}Tweets_${month}*${year}_filtered.json.tar.gz 2>/dev/null
	
	dY=`date '+%Y%m%d'`
done