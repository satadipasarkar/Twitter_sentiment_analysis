import psycopg2
import sys
import boto3

ENDPOINT="rds-postgres-free-tier.cjrnyh2qgdds.us-west-2.rds.amazonaws.com"
PORT="5432"
USERNAME="postgres"
REGION="us-west-2"
DBNAME="big_data_project"

session = boto3.session.Session()
sm_client = boto3.client(service_name='secretsmanager', region_name=REGION)
secret = sm_client.get_secret_value(SecretId='rds_postgres_free_tier')
PASSWORD = secret['SecretString']

try:
    conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user="postgres", password=PASSWORD, connect_timeout=10)
    cur = conn.cursor()
    cur.execute("""SELECT now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))