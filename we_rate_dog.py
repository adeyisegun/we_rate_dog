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
#1 remove unesecary cols, 10
df_1.sample(20)
#2 remove rows with retweet, 1
#3 remove all "tweets" that are "replies", 2
df_1.tail()
df_1.shape
df_1.columns
df_1.info()
df_1.timestamp.info()
#10 chnage timestamp dtype to datetime
#5 data types , tweet_id,denominator, 11
df_1.tweet_id.duplicated().sum()
df_1['source'].unique()
#4 extract content in source col, 3
sum(df_1.duplicated())
# examine rating denominator
df_1[df_1.rating_denominator != 10]
#6 no rating in tweet 810984652412424192, 5
#7 some tweeets with wrong ratings,6
#8 tweets with group ratings, 7
# examine rating numerator
df_1[df_1.rating_numerator < 10]
#9 tweets with decimal rating not properly extracted, 8
df_1.name.unique()
#11 convert dognames to lower case, 9
df_1.columns
# merge dog stage columns

df_2.head()
df_2.tail()
df_2.shape
df_2.columns
df_2.info()
#extract dog_breed from p_tables


#### Clean Data
# code to duplicate tables before cleaning
df_1_copy = df_1.copy()
df_2_copy = df_2.copy()
df_3_copy = df_3.copy()

#1 remove rows with retweet
df_1 = df_1[df_1['retweeted_status_id'].isnull()]

#2 remove all "tweets" that are "replies".
df_1 = df_1[df_1['in_reply_to_status_id'].isnull()]

#3 extract content in Source column
df_1['tweet_source'] = df_1.source.apply(lambda x: BeautifulSoup(x).find('a').contents[0])
df_1.tweet_source.unique()

#4 chnage timestamp dtype to datetime
df_1['timestamp'] = pd.to_datetime(df_1.timestamp)
#df_1.date_time.dt.year

#5 no rating in tweet 810984652412424192, drop row
df_1 = df_1[df_1.tweet_id != 810984652412424192]

#6 Correct rating for tweets with wrong rating
def rating_change(t_id,num,den):
    df_1.rating_numerator = np.where(df_1.tweet_id == t_id, num, df_1.rating_numerator)
    df_1.rating_denominator = np.where(df_1.tweet_id == t_id, den, df_1.rating_denominator)
rating_change(666287406224695296, 9, 10)
rating_change(740373189193256964, 14, 10)
rating_change(682962037429899265, 10, 10)
rating_change(722974582966214656, 13, 10)
rating_change(716439118184652801, 11, 10)
## test
df_1[df_1.rating_denominator != 10]

#7 find average rating for tweets with group ratings
df_1.rating_numerator = np.where(df_1.rating_denominator != 10, 
                                 df_1.rating_numerator/(df_1.rating_denominator/10), 
                                 df_1.rating_numerator)

df_1.rating_denominator = np.where(df_1.rating_denominator != 10, 10, 
                                 df_1.rating_denominator)
# then test
df_1[df_1.rating_denominator != 10]


#8 tweets with decimal rating not properly extracted
df_1['text_dec'] = df_1.text.str.extract('(\d\.\d+)+[/]10')
df_1[~df_1['text_dec'].isnull()]
rating_change(883482846933004288, 13.5, 10)
rating_change(786709082849828864, 9.75, 10)
rating_change(778027034220126208, 11.27, 10)
rating_change(680494726643068929, 11.26, 10)

#9 dognames in lower case
df_1.name = df_1.name.str.lower()

#10 remove unesecary cols
rmv_cols = ['in_reply_to_status_id', 'in_reply_to_user_id', 'retweeted_status_id',
            'retweeted_status_user_id', 'retweeted_status_timestamp', 'text_dec','source']
df_1.drop(rmv_cols, axis=1, inplace=True)

#11 data types source, tweet_id,denominator, stage
df_1['tweet_id'] = df_1['tweet_id'].astype(str)
df_1['rating_denominator'] = df_1['rating_denominator'].astype(float)
df_1['tweet_source'] = df_1['tweet_source'].astype('category')
df_2['tweet_id'] = df_2['tweet_id'].astype(str)


####### Tidy - merge dog stage columns
df_1.doggo.replace('None', '', inplace=True)
df_1.floofer.replace('None', '', inplace=True)
df_1.pupper.replace('None', '', inplace=True)
df_1.puppo.replace('None', '', inplace=True)

df_1['dog_stage'] = df_1.doggo + df_1.floofer + df_1.pupper + df_1.puppo

df_1.dog_stage.value_counts()

df_1.dog_stage.replace('doggopupper', 'doggo, pupper', inplace=True)
df_1.dog_stage.replace('doggopuppo', 'doggo, puppo', inplace=True)
df_1.dog_stage.replace('doggofloofer', 'doggo, floofer', inplace=True)
df_1.dog_stage.replace('', np.nan, inplace=True)

df_1.dog_stage.value_counts()
df_1['dog_stage'] = df_1['dog_stage'].astype('category')

#####----#extract dog_breed from p_tables------------------------------

df_2 = df_2[(df_2.p1_dog == True) | (df_2.p2_dog == True) | (df_2.p3_dog == True)]



df_2a = df_2[(df_2.p1_dog == True)]
df_2a.drop(['p1_dog', 'p2','p2_conf', 'p2_dog', 'p3', 'p3_conf', 'p3_dog'], inplace=True, axis=1)
df_2a.rename(columns={'p1': 'dog_breed', 'p1_conf': 'p_conf'}, inplace = True)

df_2b = df_2[(df_2.p1_dog == False) & (df_2.p2_dog == True)]
df_2b.drop(['p2_dog', 'p1','p1_conf', 'p1_dog', 'p3', 'p3_conf', 'p3_dog'], inplace=True, axis=1)
df_2b.rename(columns={'p2': 'dog_breed', 'p2_conf': 'p_conf'}, inplace = True)

df_2c = df_2[(df_2.p1_dog == False) & (df_2.p2_dog == False) & (df_2.p3_dog == True)]
df_2c.drop(['p3_dog', 'p1','p1_conf', 'p1_dog', 'p2', 'p2_conf', 'p2_dog'], inplace=True, axis=1)
df_2c.rename(columns={'p3': 'dog_breed', 'p3_conf': 'p_conf'}, inplace = True)

df_2 = df_2a.append(df_2b)
df_2 = df_2.append(df_2c)

'''
sddjiwjiw
'''
df_master = df_1.merge(df_2, left_on='tweet_id', right_on='tweet_id', how='left')
df_master = df_master.merge(df_3, left_on='tweet_id', right_on='id_str', how='inner')

rmv_cols = ['doggo','floofer','pupper', 'puppo', 'id_str', 'created_at']
df_master.drop(rmv_cols, axis=1, inplace=True)

df_master.drop('tweet_source', axis=1, inplace=True)


df_master.nlargest(20, 'rating_numerator')[['name','rating_numerator','dog_breed']]

df.groupby('color')['quality'].mean()


x = df_master.groupby('dog_breed')['rating_numerator'].mean().to_frame('avg_rating')
y = df_master.groupby('dog_breed')['dog_breed'].count().nlargest(5).to_frame('count_of_dog_breed')
x.merge(y, left_index = True, right_index = True, how='inner')


