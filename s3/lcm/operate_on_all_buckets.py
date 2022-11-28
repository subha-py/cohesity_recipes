from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
import urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import sys
sys.path.extend('/home/cohesity/PycharmProjects/cohesity_recipes')
import random
from itertools import cycle
import os
from multiprocessing import Pool, cpu_count

from s3.utils.bucket import (
    get_buckets_from_prefix,
    upload_custom_multi_part,
    upload_files_in_bucket,
    upload_files_in_buckets

)
from s3.utils.connector import get_s3_client, get_endpoint
from s3.utils.objects import get_random_object_keys, put_random_object_tags
from boto3.s3.transfer import TransferConfig
from cluster.connection import setup_cluster_automation_variables_in_environment
from s3.utils.aws_uploader import Chunk
import boto3





def put_tags_in_bucket(prefix='LCMTestBucket', count=200):
    client_list_cycle = get_client_cycle()
    buckets = get_buckets_from_prefix(next(client_list_cycle), prefix=prefix)
    print("Number of buckets - {}".format(len(buckets)))

    # todo: put multiprocessing here
    for bucket_name in buckets:
        keys = get_random_object_keys(next(client_list_cycle), bucket_name, count=count)
        put_random_object_tags(client_list_cycle, bucket_name, keys)

def remove_lcm_from_buckets(prefix='LCMTestBucket'):
    client_list_cycle = get_client_cycle()
    buckets = get_buckets_from_prefix(next(client_list_cycle), prefix=prefix)
    print("Number of buckets - {}".format(len(buckets)))
    for bucket_name in buckets:
        try:
            client = next(client_list_cycle)
            client.delete_bucket_lifecycle(
                Bucket=bucket_name
            )
        except Exception as ex:
            print('Could not remove lcm from bucket - {} due to {}'.format(bucket_name, ex))




if __name__ == '__main__':
    # put_tags_in_bucket('LCMTestBucket_Random_19', count=2000)
    # mpu testing
    setup_cluster_automation_variables_in_environment(cluster_ip="10.2.195.75")
    client_list_cycle = get_client_cycle()
    bucket_name = 'subha_mpu_test_0'
    # buckets = get_buckets_from_prefix(next(client_list_cycle), prefix=prefix)
    buckets = [bucket_name]
    # upload_files_in_bucket(client_list_cycle, bucket_name)
    upload_files_in_buckets(client_list_cycle, buckets)