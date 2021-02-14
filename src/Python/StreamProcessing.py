# import
from __future__ import print_function

import tweepy
import json

from bson import json_util
from kafka import KafkaProducer

# filter keyword(s)
WORDS = ["Justin Bieber"]
# specify the coordination
#  GEO = [-4.55, 55.7, -4, 56]

# (optional)save data into local file
# FILE_NAME = "tweets_data.json"

# get keys from twitter developer
CONSUMER_KEY = "Whu0n0by69wMMxxc8G6ECx4gG"
CONSUMER_SECRET = "YNwCZmbS3jvXEqTWgLHv3UDWmZkM7RXAzXe2zLRM8RSscErTFq"
ACCESS_TOKEN = "857114618-v6t0yNtdyCuHT6iMvKqJQheGmw3VBxa1SWgSkgH2"
ACCESS_TOKEN_SECRET = "Z9bDsJ4b88fvWd7yjMPNctEKuBY5bsAIr4K2izZO4GAMO"

numS = 0


# This is a class provided by tweepy to access the Twitter Streaming API.
class StreamListener(tweepy.StreamListener):

    def __init__(self, api, producer):
        # init
        self.count = 0  # count for number of tweets received
        self.api = api
        self.producer = producer
        super(tweepy.StreamListener, self).__init__()

    def on_connect(self):
        # if connect the streamer will print something
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        # When receiving data from twitter will call this method
        try:
            # (optional)write data to file
            # with open(FILE_NAME, 'a') as tf:
            #    tf.write(data)
            data_json = json.loads(data)  # Decode the JSON from Twitter

            # create kafka producer to broadcast messages
            self.producer.send('TwitterStream', json.dumps(data_json, default=json_util.default).encode('utf-8'))
            self.count = self.count+1
            # print("Tweet:  " + str(data)) print out raw data
            # print("count: "+str(self.count)) print out count number
        except Exception as e:
            print(e)


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# Set up the listener and kafka producer.
# The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True),
                          producer=KafkaProducer(bootstrap_servers='localhost:9092'))
streamer = tweepy.Stream(auth=auth, listener=listener)

print("Searching keywords are: " + str(WORDS))  # filter keywords
streamer.filter(track=WORDS, languages=['en'])
# print("Searching geo-tagged: " + str(GEO))  (optional) filter geo
# streamer.filter(locations=GEO)
