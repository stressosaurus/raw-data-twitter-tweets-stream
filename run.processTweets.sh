#!/usr/bin/env bash

# keyword streams A - 09232017
STDVID="A"
dates=("09232017")
./processTweets.py ${STDVID} "${dates[*]}" $1

# keyword streams A - 09232017-09242017
#STDVID="A"
#dates=("09232017" "09242017")
#./processTweets.py ${STDVID} "${dates[*]}" $1

# keyword streams A - 10022017-10082017
#STDVID="A"
#dates=("10022017" "10032017" "10042017" "10082017")
#./processTweets.py ${STDVID} "${dates[*]}" $1

# keyword streams A - 10102017-10142017
#STDVID="A"
#dates=("10102017" "10112017" "10122017" "10132017" "10142017" )
#./processTweets.py ${STDVID} "${dates[*]}" $1

# keyword streams A - 10152017-10192017
#STDVID="A"
#dates=("10152017" "10162017" "10172017" "10182017" "10192017")
#./processTweets.py ${STDVID} "${dates[*]}" $1
