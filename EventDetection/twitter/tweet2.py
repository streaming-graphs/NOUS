#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tweepy
import datetime
import time

# credentials from https://apps.twitter.com/
consumerKey = ""
consumerSecret = ""
accessToken = ""
accessTokenSecret = ""

auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessToken, accessTokenSecret)

api = tweepy.API(auth)
tweets = api.search(q='lion', geocode='39.8,-95.583068847656,1500mi')
for tweet in tweets:
    print tweet.text


startDate = datetime.datetime(2014, 6, 1, 0, 0, 0)
endDate = datetime.datetime(2014, 7, 1, 0, 0, 0)

tweets = []
tmpTweets = api.search(q='', geocode='39.8,-95.583068847656,1500mi')
for tweet in tmpTweets:
    if startDate < tweet.created_at < endDate:
        tweets.append(tweet)

# while tmpTweets[-1].created_at > startDate:
while len(tweets) < 10:
    try:
        tmpTweets = api.search(q='', geocode='39.8,-95.583068847656,1500mi', max_id=tmpTweets[-1].id)
        for tweet in tmpTweets:
            if startDate < tweet.created_at < endDate:
                tweets.append(tweet)
    except tweepy.error.RateLimitError:
        print 'Pausing'
        time.sleep(999)

for tweet in tweets:
    print tweet.text
