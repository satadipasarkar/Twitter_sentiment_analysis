import json
import time

import psycopg2
import boto3
import requests as requests
from psycopg2._json import Json
from psycopg2.extensions import register_adapter

from psycopg2.extras import execute_values

STAGE = 'prod'  # 'prod'
CONFIG = {
    'test': {
        'table_name': 'twitter_sentiments_test',
        'bucket_name': 'ism-6362-test',
        'batch_size': 2
    },
    'prod': {
        'table_name': 'twitter_sentiments',
        'bucket_name': 'ism-6362-test',
        'batch_size': 25
    }
}

TWITTER_SENTIMENTS_TABLE_NAME = CONFIG[STAGE]['table_name']
BUCKET_NAME = CONFIG[STAGE]['bucket_name']
BATCH_SIZE = CONFIG[STAGE]['batch_size']

ENDPOINT = "rds-postgres-free-tier.cjrnyh2qgdds.us-west-2.rds.amazonaws.com"
PORT = "5432"
USERNAME = "postgres"
REGION = "us-west-2"
DBNAME = "big_data_project"

session = boto3.session.Session()
sm_client = boto3.client(service_name='secretsmanager', region_name=REGION)
rds_secret = sm_client.get_secret_value(SecretId='rds_postgres_free_tier')
PASSWORD = rds_secret['SecretString']

twitter_secret = sm_client.get_secret_value(SecretId='twitter/sentiment_analysis/ism_6362')
twitter_secret_json = json.loads(twitter_secret['SecretString'])
TWITTER_CLIENT_ID = twitter_secret_json['client_id']
TWITTER_CLIENT_SECRET = twitter_secret_json['client_secret']

conn = None
s3_client = None

register_adapter(dict, Json)


def init_connection_to_database():
    try:
        global conn
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user="postgres", password=PASSWORD,
                                connect_timeout=10)
    except Exception as e:
        print("Database connection failed due to {}".format(e))


def init_connection_to_s3():
    global s3_client
    s3_client = boto3.client('s3')


def create_table(table_name):
    print(f'Creating table {table_name} if not exists.')
    init_connection_to_database()
    cur = conn.cursor()
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS "{table_name}" ("tweet_id" varchar PRIMARY KEY, "tweet_response" json,"tweet_text" varchar,"sentiment_score_positive" numeric,"sentiment_score_negative" numeric,"sentiment_score_neutral" numeric,"sentiment_score_mixed" numeric,"sentiment" varchar,"workflow_status" varchar); ')
    # print(query_results)
    conn.commit()
    cur.close()
    conn.close()


def get_objcets_from_s3(bucket_name):
    init_connection_to_s3()
    object_list = []
    for key in s3_client.list_objects(Bucket=bucket_name)['Contents']:
        object_list.append(key['Key'])
    return object_list


def read_content_from_s3(bucket_name, key):
    print(f'Reading file from s3://{bucket_name}/{key}')
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = s3_object['Body']
    file_contents = body.read().decode("utf-8").split('\n')
    tweet_ids = []
    for line in file_contents:
        if len(line) > 0:
            tweet_ids.append(line)
    return tweet_ids


def store_tweet_ids_in_database(table_name, tweet_ids):
    init_connection_to_database()
    data = []
    for tweet_id in tweet_ids:
        data.append((tweet_id, 'NOT_STARTED'))
    try:
        cur = conn.cursor()
        execute_values(cur, f'INSERT INTO {table_name} (tweet_id, workflow_status) VALUES %s', data)
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        pass
    finally:
        conn.close()
    print(f'Inserted {len(tweet_ids)} tweet_ids into database.')


def get_unprocessed_tweet_ids(table_name, workflow_status, batch_size):
    init_connection_to_database()
    cur = conn.cursor()
    cur.execute(
        f'SELECT tweet_id, tweet_text, tweet_response, workflow_status from {table_name} where workflow_status=\'{workflow_status}\' order by tweet_id limit {batch_size}')
    # print(query_results)
    query_results = cur.fetchall()
    conn.close()
    return query_results


def get_twitter_auth_token():
    oauth_response = requests.post("https://api.twitter.com/oauth2/token",
                                   data={'grant_type': 'client_credentials', 'client_id': TWITTER_CLIENT_ID,
                                         'client_secret': TWITTER_CLIENT_SECRET})
    oauth_json = json.loads(oauth_response.text)
    auth_token = oauth_json['access_token']
    return auth_token


access_token = get_twitter_auth_token()


def get_tweet(tweet_id):
    global access_token
    retry_count = 0
    while retry_count < 3:
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        response = requests.get(f'https://api.twitter.com/1.1/statuses/show.json?id={tweet_id}', headers=headers)
        if response.status_code == 401:
            access_token = get_twitter_auth_token()
        elif response.status_code == 200:
            return response
        elif response.status_code == 429:
            sleep_time = 50
            print(f'Throttled by Twitter. Sleeping for {sleep_time}.')
            time.sleep(sleep_time)
        else:
            return None
        retry_count += 1

    return None


def update_tweets_table_with_tweets(table_name, values):
    init_connection_to_database()
    cur = conn.cursor()
    cur.executemany(
        f'UPDATE {table_name} SET tweet_response = %s, tweet_text = %s, workflow_status = %s WHERE tweet_id = %s',
        values)
    conn.commit()
    conn.close()
    print(f'Updated {len(values)} tweets in database.')


def get_sentiments(text_list, tweet_to_tweet_id_map, language_code):
    if k not in ['ar', 'hi', 'ko', 'zh-TW', 'ja', 'zh', 'de', 'pt', 'en', 'it', 'fr', 'es']:
        sentiments = []
        for text in text_list:
            tweet_id = tweet_to_tweet_id_map[text]
            sentiments.append({
                'sentiment': 0,
                'sentiment_score_positive': 0,
                'sentiment_score_negative': 0,
                'sentiment_score_mixed': 0,
                'sentiment_score_neutral': 0,
                'tweet_id': tweet_id,
                'workflow_status': 'COMPLETED_WITH_COMPREHEND_LANGUAGE_NOT_SUPPORTED'
            })
        return sentiments

    comprehend_client = boto3.client('comprehend', 'us-west-2')
    response = comprehend_client.batch_detect_sentiment(
        TextList=text_list,
        LanguageCode=language_code
    )

    sentiments = []
    for sentiment in response['ResultList']:
        index = sentiment['Index']
        tweet_text = text_list[index]
        tweet_id = tweet_to_tweet_id_map[tweet_text]
        sentiment_score = sentiment['SentimentScore']
        sentiments.append({
            'sentiment': sentiment['Sentiment'],
            'sentiment_score_positive': sentiment_score['Positive'],
            'sentiment_score_negative': sentiment_score['Negative'],
            'sentiment_score_mixed': sentiment_score['Mixed'],
            'sentiment_score_neutral': sentiment_score['Neutral'],
            'tweet_id': tweet_id,
            'workflow_status': 'COMPLETED'
        })

    for errored in response['ErrorList']:
        index = errored['Index']
        tweet_text = text_list[index]
        tweet_id = tweet_to_tweet_id_map[tweet_text]
        sentiments.append({
            'sentiment': 0,
            'sentiment_score_positive': 0,
            'sentiment_score_negative': 0,
            'sentiment_score_mixed': 0,
            'sentiment_score_neutral': 0,
            'tweet_id': tweet_id,
            'workflow_status': 'COMPLETED_WITH_COMPREHEND_ERROR'
        })
    return sentiments


def update_tweets_table_with_sentiments(table_name, values):
    init_connection_to_database()
    cur = conn.cursor()
    cur.executemany(
        f'UPDATE {table_name} SET sentiment = %s, sentiment_score_positive = %s, sentiment_score_negative = %s, sentiment_score_neutral = %s, sentiment_score_mixed = %s, workflow_status = %s WHERE tweet_id = %s',
        values)
    conn.commit()
    conn.close()
    print(f'Updated {len(values)} tweets with sentiments in database.')


if __name__ == '__main__':
    init_connection_to_database()
    create_table(TWITTER_SENTIMENTS_TABLE_NAME)
    tweet_id_files = get_objcets_from_s3(BUCKET_NAME)
    for f in tweet_id_files:
        tweet_ids = read_content_from_s3(BUCKET_NAME, f)
        store_tweet_ids_in_database(table_name=TWITTER_SENTIMENTS_TABLE_NAME, tweet_ids=tweet_ids)
    while True:
        unprocessed_tweet_ids = get_unprocessed_tweet_ids(table_name=TWITTER_SENTIMENTS_TABLE_NAME,
                                                          workflow_status='NOT_STARTED', batch_size=BATCH_SIZE)
        if len(unprocessed_tweet_ids) == 0:
            break
        unprocessed_tweets = []
        for tweet_id in unprocessed_tweet_ids:
            tweet_response = get_tweet(tweet_id[0])
            if tweet_response is None:
                unprocessed_tweets.append(({}, '', 'TWEET_NOT_FOUND', tweet_id[0]))
            elif tweet_response.status_code == 200:
                tweet_response_json = json.loads(tweet_response.content)
                unprocessed_tweets.append(
                    (tweet_response_json, tweet_response_json['text'], 'TWEET_FETCHED', tweet_id[0]))
            else:
                raise Exception()
        update_tweets_table_with_tweets(table_name=TWITTER_SENTIMENTS_TABLE_NAME, values=unprocessed_tweets)

    while True:
        unprocessed_tweets = get_unprocessed_tweet_ids(table_name=TWITTER_SENTIMENTS_TABLE_NAME,
                                                       workflow_status='TWEET_FETCHED', batch_size=BATCH_SIZE)
        if len(unprocessed_tweets) == 0:
            break

        unprocessed_tweets_language_code_dict = {}
        tweet_text_to_tweet_id_dict = {}
        for unprocessed_tweet in unprocessed_tweets:
            lang_code = unprocessed_tweet[2]['lang']

            if lang_code not in unprocessed_tweets_language_code_dict:
                unprocessed_tweets_language_code_dict[lang_code] = []

            tweet_text = unprocessed_tweet[1]
            unprocessed_tweets_language_code_dict[lang_code].append(tweet_text)
            tweet_text_to_tweet_id_dict[tweet_text] = unprocessed_tweet[0]

        for k in unprocessed_tweets_language_code_dict:
            sentiment_response = get_sentiments(text_list=unprocessed_tweets_language_code_dict[k],
                                                tweet_to_tweet_id_map=tweet_text_to_tweet_id_dict,
                                                language_code=k)

            update_sentiment_values = []
            for sentiment in sentiment_response:
                update_sentiment_values.append((sentiment['sentiment'], sentiment['sentiment_score_positive'],
                                                sentiment['sentiment_score_negative'],
                                                sentiment['sentiment_score_neutral'],
                                                sentiment['sentiment_score_mixed'], sentiment['workflow_status'],
                                                sentiment['tweet_id']))

            update_tweets_table_with_sentiments(table_name=TWITTER_SENTIMENTS_TABLE_NAME,
                                                values=update_sentiment_values)
