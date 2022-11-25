from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
import urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import random
from itertools import cycle
import os

from s3.utils.bucket import (
    get_buckets_from_prefix,
    upload_custom_multi_part
)
from s3.utils.connector import get_s3_client, get_endpoint
from s3.utils.objects import get_random_object_keys, put_random_object_tags
from boto3.s3.transfer import TransferConfig
from cluster.connection import setup_cluster_automation_variables_in_environment
from s3.utils.aws_uploader import Chunk
import boto3


def get_client_cycle():
    ips = os.environ.get("node_ips").split(",")
    client_list = []
    for ip in ips:
        client = get_s3_client(get_endpoint(ip), os.environ.get("s3AccessKeyId"), os.environ.get("s3SecretKey"))
        if client:
            client_list.append(client)
    return cycle(client_list)


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
    local_file = '/home/cohesity/FioFiles/size_100m'
    chunksizes = [2,4,8,16,24]
    upload_custom_multi_part(client_list_cycle, bucket_name, local_file, remote_file_path='fioFile/size_100m',
                             chunk_size_mib=random.choice(chunksizes))
