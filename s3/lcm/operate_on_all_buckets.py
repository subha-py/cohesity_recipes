import sys
sys.path.extend(['/home/cohesity/PycharmProjects/cohesity_recipes'])
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
import urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from s3.utils.bucket import (
    get_buckets_from_prefix,
    upload_files_in_buckets,
    overwrite_files_in_bucket
)
from s3.utils.objects import get_random_object_keys, put_random_object_tags
from cluster.connection import setup_cluster_automation_variables_in_environment, get_client_cycle
from multiprocessing import Pool, cpu_count
import concurrent.futures


def put_tags_in_bucket(prefix='LCMTestBucket', count=200):
    client_list_cycle = get_client_cycle()
    buckets = get_buckets_from_prefix(next(client_list_cycle), prefix=prefix)
    print("Number of buckets - {}".format(len(buckets)))

    # todo: put multiprocessing here
    for bucket_name in buckets:
        keys = get_random_object_keys(next(client_list_cycle), bucket_name, count=count)
        if keys:
            put_random_object_tags(bucket_name, keys)

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

def run_tag_workload(cluster_ip, prefix, count=200, password='Syst7mt7st'):
    def run_tag(bucket, count):
        keys = get_random_object_keys(next(client_cycle), bucket, count=count)
        put_random_object_tags(bucket_name=bucket, keys=keys)

    setup_cluster_automation_variables_in_environment(cluster_ip=cluster_ip, password=password)
    client_cycle = get_client_cycle()
    buckets = get_buckets_from_prefix(next(client_cycle), prefix=prefix)
    future_to_bucket= {}
    if not buckets:
        return
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(buckets)) as executor:
        for bucket in buckets:
            arg = (bucket, count)
            future_to_bucket[executor.submit(run_tag, *arg)] = bucket

    for future in concurrent.futures.as_completed(future_to_bucket):
        bucket = future_to_bucket[future]
        try:
            res = future.result()
        except Exception as exc:
            print("%r generated an exception: %s" % (bucket, exc))
        else:
            print("bucket tags are placed - {}".format(bucket))

if __name__ == '__main__':
    # put_tags_in_bucket('LCMTestBucket_Random_19', count=2000)
    # mpu testing

    while True:
        run_tag_workload(cluster_ip="10.14.29.182",prefix="LCMTestBucket_", password='admin')