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

    # shared read-only object bucket containing datasets
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
    
    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ethereum-parvulus/transactions.csv")
    header = lines.first()
    clean_sample = lines.filter(lambda x: x != header)
    # Monthly Transactions
    monthly = clean_sample.map(lambda x: (datetime.fromtimestamp(int(x.split(",")[11])).strftime("%m/%Y"), 1))
    monthly = monthly.reduceByKey(operator.add)
    # Average Transactions
    average = clean_sample.map(lambda x: (datetime.fromtimestamp(int(x.split(",")[11])).strftime("%m/%Y"), int(x.split(",")[7])))
    record = average.count()
    average = average.reduceByKey(operator.add)
    average = average.map(lambda x: (x[0], format(x[1]/record, '.2f')))
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/monthly_transactions.txt')
    my_result_object.put(Body=json.dumps(monthly.collect()))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/average_transactions.txt')
    my_result_object.put(Body=json.dumps(average.collect()))
    
    spark.stop()
