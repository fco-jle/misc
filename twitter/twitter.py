import tweepy
import pandas as pd
import os

# Variables that contains the credentials to access Twitter API
creds = pd.read_csv(os.path.join(os.path.dirname(__file__), 'tweeter_creds.pwd'), sep=',')
creds = creds.iloc[0]
ACCESS_TOKEN = creds['ACCESS_TOKEN']
ACCESS_SECRET = creds['ACCESS_SECRET']
CONSUMER_KEY = creds['CONSUMER_KEY']
CONSUMER_SECRET = creds['CONSUMER_SECRET']


class Tweet:
    def __init__(self, status):
        self.created_at = status.created_at
        self.author_name = status.author.name
        self.author_screen_name = status.author.screen_name
        self.author_location = status.author.location
        self.author_lang = status.author.lang
        self.lang = status.lang

        self.id_ = status.id
        self.text = status.text

        self.author_geo_enabled = status.author.geo_enabled
        self.author_id = status.author.id
        self.coordinates = status.coordinates
        self.geo = status.geo
        self.retweet_count = status.retweet_count
        self.source = status.source
        self.source_url = status.source_url

    def to_dict(self):
        tweet_dict = dict(author_geo_enabled=self.author_geo_enabled,
                          author_id=self.author_id,
                          author_lang=self.author_lang,
                          author_location=self.author_location,
                          author_name=self.author_name,
                          author_screen_name=self.author_screen_name,
                          coordinates=self.coordinates,
                          created_at=self.created_at,
                          geo=self.geo,
                          id_=self.id_,
                          lang=self.lang,
                          retweet_count=self.retweet_count,
                          source=self.source,
                          source_url=self.source_url,
                          text=self.text)
        return tweet_dict


class TweeterListener(tweepy.StreamListener):
    def __init__(self, topic):
        super(TweeterListener, self).__init__()
        self.tweet_count = 0
        self.tweets_list = []
        self.topic = topic

    def on_status(self, status):
        if self.tweet_count > 100:
            return False
        tweet = Tweet(status)
        self.tweets_list.append(tweet.to_dict())
        self.tweet_count += 1
        print(tweet.text)

    def on_error(self, status_code):
        # code to run each time an error is received
        if status_code == 420:
            return False
        else:
            return True

    def save_dataframe(self):
        tweets_df = pd.DataFrame.from_records(self.tweets_list)
        tweets_df.to_csv(os.path.join(os.path.dirname(__file__), f'tweets_{self.topic}.csv'))


# Setup access to API
def connect_to_twitter():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth)
    return api


if __name__ == '__main__':
    con = connect_to_twitter()
    tweepy_listener = TweeterListener(topic='covid')
    tweepy_stream = tweepy.Stream(auth=con.auth, listener=tweepy_listener)
    tweepy_stream.filter(track=['covid-19'])
    tweepy_listener.save_dataframe()
