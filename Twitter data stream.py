import tweepy
import time
from datetime import timedelta
from kafka import KafkaConsumer, KafkaProducer, consumer
from datetime import datetime

#twitter setup
consumer_key = "Ravt8ILnJBEMEUryvTk4QWaF0"
consumer_secret = "4UshTvCfoOef7FiNrx021DzbCCnhQEMeoQzt06dBJPgClAlq8V"
access_token = "1337197981108379648-iPXDmLxsfv3pYVlVRe756cAKHep686"
access_token_secret = "IWm5wU4vFMlT3Gx3aDcWZpr7ETZT4PrH8jMj05k3kGVYX"

#Autentikasi twitter developer
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
topic_name = 'uji'

#get Data
def get_twitter_data():
    res = api.search('Tokopedia')
    count = 1
    for i in res:
        record = ''
        record += str(i.user.id_str)
        record += '~'
        record += str(i.text)
        record += '~'
        record += str(normalize_timestamp(str(i.created_at)))
        record += '~'
        print("get ", count, " tweet")
        producer.send(topic_name, str.encode(record))
        count += 1

def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)

periodic_work(5*1)