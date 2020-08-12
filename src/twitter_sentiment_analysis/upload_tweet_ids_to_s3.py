folder_path = "/Users/satadipa/Downloads/twitter_sentiment_analysis/twitter-events-2012-2016"

import gzip
import logging
import boto3
import shutil
from botocore.exceptions import ClientError
from os import listdir
from os.path import isfile, join

ACCESS_KEY = ""
SECRET_KEY = ""

s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


BUCKET_NAME = "ism-6362"
FILE_NAME = "2012-mexican-election.ids.gz"


def exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def extract_and_upload(file_to_upload):
    temp_file_name = file_to_upload.split('/')[-1][:-3]
    if exists(bucket=BUCKET_NAME, key=temp_file_name):
        print(f'{temp_file_name} already exists. Skipping!')
        return

    # print(f'{temp_file_name}, {file_to_upload}')
    with gzip.open(file_to_upload, 'r') as in_file:
        with open(f'/tmp/{temp_file_name}', 'wb') as out_file:
            shutil.copyfileobj(in_file, out_file)
    upload_file(f'/tmp/{temp_file_name}', bucket=BUCKET_NAME, object_name=temp_file_name)
    print(f'uploaded {file_to_upload}')


if __name__ == '__main__':
    # bucket_created = create_bucket(BUCKET_NAME)
    onlyfiles = [f for f in listdir(folder_path) if isfile(join(folder_path, f))]
    for f in onlyfiles:
        extract_and_upload(join(folder_path, f))
