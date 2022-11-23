from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
import urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from s3.utils.aws_highlvlapi.aws_transfer_manager import TransferCallback as aws_TransferCallback

import random
from itertools import cycle
import os

from s3.utils.bucket import get_buckets_from_prefix
from s3.utils.connector import get_s3_client, get_endpoint
from s3.utils.objects import get_random_object_keys, put_random_object_tags
from boto3.s3.transfer import TransferConfig
from cluster.connection import setup_cluster_automation_variables_in_environment
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

def get_fileSize(file_path):
    """
    Get the size of a file and return in Bytes
    :param file_path:
    :return: size of file in Bytes
    """
    # file size bytes
    return os.path.getsize(file_path)



if __name__ == '__main__':
    # put_tags_in_bucket('LCMTestBucket_Random_19', count=2000)
    # put_file_to_bucket_without_mpu(destpath="s3://subha_mpu_test_0/",
    #                             origpath='/home/cohesity/PycharmProjects/cohesity_recipes/s3/lcm/documents',
    #                             access_key=access_key,
    #                             secret_access_key=secret_access_key,
    #                             endpoint=get_endpoint('10.2.199.137'),
    #                             )
    # mpu testing
    setup_cluster_automation_variables_in_environment(cluster_ip="10.2.195.75")
    client_list_cycle = get_client_cycle()
    client = next(client_list_cycle)
    config = TransferConfig(multipart_chunksize=1024)
    origin_path= 'test_files/file_size_61_mb.dmg'
    callback = aws_TransferCallback(get_fileSize(origin_path))
    client.upload_file(origin_path,'subha_mpu_test_0','mpu/small', Config=config,
                       Callback=callback)
    # mpu testing end
