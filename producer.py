from pykafka import KafkaClient
import tweepy
import json
from requests_aws4auth import AWS4Auth
from credentials import consumer_key, consumer_secret, access_token, access_token_secret

class StreamListener(tweepy.StreamListener):
    def __init__(self):
        super(StreamListener, self).__init__()
        client = KafkaClient(hosts="127.0.0.1:9092")
        self.topic = client.topics['Tweets']

    def on_status(self, status):#for every tweet
        try:
            json_data = status._json
            if json_data['coordinates'] is not None:
                print('@' + json_data['user']['screen_name'])
                print(json_data['text'].lower().encode('ascii', 'ignore').decode('ascii'))
                # create JSON object of relevant content from response
                filtered = {
                    'text': json_data['text'].lower().encode('ascii', 'ignore').decode('ascii'),
                    'handle': json_data['user']['screen_name']
                }
                # add tweet to kafka
                with self.topic.get_sync_producer() as producer:
                    producer.produce('Tweet @{}:- {}\n'.format(filtered['handle'], filtered['text']))

        except Exception as e:
            print(e)
            pass

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
GEOBOX_WORLD= [-180,-90,180,90]
streamer = tweepy.Stream(auth=auth, listener=StreamListener(), timeout=30000)
streamer.filter(locations=GEOBOX_WORLD)
