#!/usr/bin/env bash

# index tabulated tweets for subsetting
./indexTweets.py 'A' 'hashtag' 'a' '09232017-09242017' 100000 '09232017-09242017'
#./indexTweets.py 'A' 'hashtag' 'a' '09232017-10192017' 100000 '09232017-10192017'

# subset tabulated tweet files
./subsetTweets.py 'A' 'hashtag' 'a' '09232017-09232017' 100000 '09232017-09242017'
./subsetTweets.py 'A' 'hashtag' 'a' '09232017-09242017' 100000 '09232017-09242017'
#./subsetTweets.py 'A' 'hashtag' 'a' '09232017-09232017' 100000 '09232017-10192017'
#./subsetTweets.py 'A' 'hashtag' 'a' '09232017-09242017' 100000 '09232017-10192017'
#./subsetTweets.py 'A' 'hashtag' 'a' '10022017-10082017' 100000 '09232017-10192017'
#./subsetTweets.py 'A' 'hashtag' 'a' '10102017-10142017' 100000 '09232017-10192017'
#./subsetTweets.py 'A' 'hashtag' 'a' '10152017-10192017' 100000 '09232017-10192017'

# merge subset tweets
./mergeTweets.py 'A' 'hashtag' 'a' '09232017-09242017'
#./mergeTweets.py 'A' 'hashtag' 'a' '09232017-10192017'
