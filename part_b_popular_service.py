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
    
    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ethereum-parvulus/transactions.csv")
    header = lines.first()
    clean_sample = lines.filter(lambda x: x != header)
    transactions = clean_sample.map(lambda x: (x.split(",")[6], int(x.split(",")[7]))).reduceByKey(operator.add)
    
    # popular service
    join_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ethereum-parvulus/contracts.csv")
    c_header = join_lines.first()
    clean_contracts = join_lines.filter(lambda x: x != c_header)
    contracts = clean_contracts.map(lambda x: (x.split(",")[0], 1))
    joined_data = transactions.join(contracts)
    top10_popular_service = joined_data.takeOrdered(10, key=lambda x: -x[1][0])
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/top_10_popular_services.txt')
    my_result_object.put(Body=json.dumps(top10_popular_service))
    
    spark.stop()

