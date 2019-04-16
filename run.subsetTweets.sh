#!/usr/bin/env bash

# subset tabulated tweet files
./subsetTweets.py 'A' 'hashtag' 'a' '09232017-09232017' 100000
./subsetTweets.py 'A' 'hashtag' 'a' '09232017-09242017' 100000
#./subsetTweets.py 'A' 'hashtag' 'a' '10022017-10082017' 100000
#./subsetTweets.py 'A' 'hashtag' 'a' '10102017-10142017' 100000
#./subsetTweets.py 'A' 'hashtag' 'a' '10152017-10192017' 100000

# merge mergeTweets
./mergeTweets.py 'A' 'subset' '09232017-09242017' 'hashtag' 'a'
#./mergeTweets.py 'A' 'subset' '09232017-10192017' 'hashtag' 'a'
