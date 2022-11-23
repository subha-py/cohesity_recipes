#!/usr/bin/python
# -*- coding: utf-8 -*-
import boto3

# import logging
import time
from botocore.config import Config
import random
import ssl
access_key = "3_PFcp4W8hhX4B9Q8csnTgUSt5th7TcxAgfwONBAwxM"
secret_access_key = "EjvB-J89QcUFVPLXu1aORCQM-4oYW4QMVmiCr2eeQJs"
endpoint = "https://10.14.29.182:3000"
bucket_name = "LCMTestBucket_Hierarchical_19"

# Disable retry.
config = Config(retries={"max_attempts": 1, "mode": "standard"})
client = boto3.client(
    service_name="s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key,
    endpoint_url=endpoint,
    verify=False,
    config=config
)
'/home/cohesity/opt/go/bin:/home/cohesity//bin:/home/cohesity/opt/go/bin:/home/cohesity/opt/go/bin:/home/cohesity//bin:/home/cohesity/opt/go/bin:/home/cohesity/.local/bin:/home/cohesity/opt/go/bin:/home/cohesity//bin:/home/cohesity/opt/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/cohesity/software/toolchain/x86_64-linux/6.3/bin:/home/cohesity/software/toolchain/x86_64-linux/6.3/bin:/home/cohesity/software/toolchain/x86_64-linux/6.3/bin'
# Upload multiple versions of an object
key = "foo/bar"
body = "abcdefghijklmnopqrstuvwxyz"
# num_objects = 1035
num_objects = 1
for i in range(1, (num_objects + 1)):
    # try:
    body = list(body)
    random.shuffle(body)
    body = "".join(body)
    client.put_object(Bucket=bucket_name, Key=key, Body=body)
    print("Put object succeeded, i = " + str(i))
    # except Exception as ex:
    #     print(ex)
    #     print("Put object failed, i = " + str(i))
    #     time.sleep(15)
