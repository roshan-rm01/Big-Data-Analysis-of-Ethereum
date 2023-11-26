import sys, string
import os
import socket
import time
import operator
import boto3
import json
import math
import time
import operator
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ethereum-parvulus/blocks.csv")
    header = lines.first()
    clean_sample = lines.filter(lambda x: x != header)
    
    # Data Overhead
    # remove first 2 bits -> take size/length of hex string -> multiply by 4 -> total number of bits
    # do instructions for logs_bloom, sha3_uncles, transactions_root, state_root, receipts_root .map(lambda x: ("logs_bloom", len(x.split(",")[5][2:])*4))
    # add all bytes up and get size of transaction file and calculate difference
    logs_bloom = clean_sample.map(lambda x: ("logs_bloom", len(x.split(",")[5][2:])*4)).reduceByKey(operator.add)
    sha3_uncles = clean_sample.map(lambda x: ("sha3_uncles", len(x.split(",")[4][2:])*4)).reduceByKey(operator.add)
    transactions_root = clean_sample.map(lambda x: ("transactions_root", len(x.split(",")[6][2:])*4)).reduceByKey(operator.add)
    state_root = clean_sample.map(lambda x: ("state_root", len(x.split(",")[7][2:])*4)).reduceByKey(operator.add)
    receipt_root = clean_sample.map(lambda x: ("receipt_root", len(x.split(",")[8][2:])*4)).reduceByKey(operator.add)
    
    file_size = boto3.session.Session().client(service_name="s3", aws_access_key_id=s3_access_key_id,
                                               aws_secret_access_key=s3_secret_access_key,
                                               endpoint_url=os.environ['S3_ENDPOINT_URL']).head_object(Bucket="data-repository-bkt", Key="ethereum-parvulus/transactions.csv")
    print(file_size['ContentLength'])
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/logs_bloom.txt')
    my_result_object.put(Body=json.dumps(logs_bloom.take(1)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/sha3_uncles.txt')
    my_result_object.put(Body=json.dumps(sha3_uncles.take(1)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/transactions_root.txt')
    my_result_object.put(Body=json.dumps(transactions_root.take(1)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/state_root.txt')
    my_result_object.put(Body=json.dumps(state_root.take(1)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/receipt_root.txt')
    my_result_object.put(Body=json.dumps(receipt_root.take(1)))
    
    spark.stop()
