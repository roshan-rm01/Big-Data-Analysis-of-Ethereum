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
    
    join_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ethereum-parvulus/contracts.csv")
    c_header = join_lines.first()
    clean_contracts = join_lines.filter(lambda x: x != c_header)
    
    gas_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ethereum-parvulus/transactions.csv")
    gas_head = gas_lines.first()
    clean_gas = gas_lines.filter(lambda x: x != gas_head)
    # Gas price over time
    gas_price = clean_gas.map(lambda x: (datetime.fromtimestamp(int(x.split(",")[11])).strftime("%m/%Y"), int(x.split(",")[9]))).reduceByKey(operator.add)
    list_contract = clean_contracts.map(lambda x: (x.split(",")[0], 1))
    
    contracts = clean_gas.map(lambda x: (x.split(",")[6], (datetime.fromtimestamp(int(x.split(",")[11])).strftime("%m/%Y"), int(x.split(",")[8]))))
    joined = contracts.join(list_contract)
    # Gas used for contract transactions over time
    contract_time = joined.map(lambda x: (x[1][0][0], x[1][0][1])).reduceByKey(operator.add)
    
    # Average gas used for contract transactions
    avg_gas_used = joined.map(lambda x: ("gas_used", x[1][0][1]))
    gas_used_record = avg_gas_used.count()
    avg_gas_used = avg_gas_used.reduceByKey(operator.add)
    avg_gas_used = avg_gas_used.map(lambda x: (x[0], x[1]/gas_used_record))
    
    # Gas Used from the ten Popular Smart Contracts
    popular = ["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444",
"0x7727e5113d1d161373623e5f49fd568b4f543a9e",
"0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef",
"0xfa52274dd61e1643d2205169732f29114bc240b3",
"0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8",
"0xbfc39b6f805a9e40e77291aff27aee3c96915bdd",
"0xe94b04a0fed112f3664e45adb2b8915693dd5ff3",
"0xbb9bc244d798123fde783fcc1c72d3bb8c189413",
"0xabbb6bebfa05aa13e908eaa492bd7a8343760477",
"0x341e790174e3a4d35b65fdc067b6b5634a61caea"]
    filt_contract = clean_contracts.filter(lambda x: x.split(",")[0] in popular)
    new_list_contract = filt_contract.map(lambda x: (x.split(",")[0], 1))
    
    contracts = clean_gas.map(lambda x: (x.split(",")[6], (datetime.fromtimestamp(int(x.split(",")[11])).strftime("%m/%Y") ,int(x.split(",")[8]))))
    new_joined = contracts.join(new_list_contract)
    
    popular_contract_gas = new_joined.map(lambda x: (x[0], x[1][0][1])).reduceByKey(operator.add)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/contract_gas_used.txt')
    my_result_object.put(Body=json.dumps(contract_time.collect()))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/average_gas_used.txt')
    my_result_object.put(Body=json.dumps(avg_gas_used.take(1)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/popular_gas_used.txt')
    my_result_object.put(Body=json.dumps(popular_contract_gas.take(10)))
    
    spark.stop()
