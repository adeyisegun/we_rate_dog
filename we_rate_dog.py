import numpy as np
import pandas as pd
import requests
import tweepy
import json
from timeit import default_timer as timer
from bs4 import BeautifulSoup

url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'

### Reding the twitter achieve, df_1
df_1 = pd.read_csv('twitter-archive-enhanced.csv')

###  Read tweet image predictions, df_2
response = requests.get(url)
# check response
response.status_code == 200
# Save TSV to file
with open("image_predictions.tsv", mode='wb') as file:
    file.write(response.content)
    df_2 = pd.read_csv('image_predictions.tsv', sep='\t')

### Query twitter data from Twitter API, df_3
# --------------------API Keys, Secrets, and Tokens--------------------
# Query Twitter API for each tweet in the Twitter archive and save JSON in a text file
# These are hidden to comply with Twitter's API terms and conditions
consumer_key = 'HIDDEN'
consumer_secret = 'HIDDEN'
access_token = 'HIDDEN'
access_secret = 'HIDDEN'
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)
# -----Query Twitter's API for JSON data for each tweet ID in the Twitter archive--------
start = timer()
df_list = []
# Save each tweet's returned JSON as a new line in a .txt file
with open('tweet_json.txt', 'w') as file:
    # This loop will likely take 20-30 minutes to run because of Twitter's rate limit
    for tweet_id in df_1['tweet_id']:
        try:
            tweet = api.get_status(tweet_id, tweet_mode = 'extended')
            json.dump(tweet._json, file)
            file.write('\n')
            print (tweet_id, 'success')
            df_list.append({'tweet_id': tweet_id, 'Status': 'Success'})
        except:
            print (tweet_id, 'Failed')
            df_list.append({'tweet_id': tweet_id, 'Status': 'Failed'})
            pass
tweet_status = pd.DataFrame(df_list, columns = ['tweet_id', 'Status'])
end = timer()
print(end - start)
print(tweet_status.groupby('Status').count())
# --------------------*****************************--------------------

df2_list = []
with open('tweet_json.txt') as file:
    for line in file.readlines():
        json_data = json.loads(line)
        df2_list.append(json_data)
df_3 = pd.DataFrame(df2_list, columns = ['id_str', 'created_at', 
                                         'retweet_count', 'favorite_count'])

#### Accessing Data
df_1.head()
#1 remove unesecary cols
df_1.sample(20)
#2 remove rows with retweet
#3 remove all "tweets" that are "replies".
df_1.tail()
df_1.shape
df_1.columns
df_1.info()
df_1.timestamp.info() 
#10 chnage timestamp dtype to datetime
#5 data types , tweet_id,denominator
df_1.tweet_id.duplicated().sum()
df_1['source'].unique()
#4 extract content in source col
sum(df_1.duplicated())
# examine rating denominator
df_1[df_1.rating_denominator != 10]
#6 no rating in tweet 810984652412424192
#7 some tweeets with wrong ratings
#8 tweets with group ratings
# examine rating numerator
df_1[df_1.rating_numerator < 10]
#9 tweets with decimal rating not properly extracted
df_1.name.unique()
#11 convert dognames to lower case
df_1.columns
# merge dog stage columns

df_2.head()
df_2.tail()
df_2.shape
df_2.columns
df_2.info()
#extract dog_breed from p_tables