import requests
import urllib3
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from s3.utils.bucket import (
    get_buckets_from_prefix,
)
from cluster.connection import get_client_cycle
from cluster.connection import setup_cluster_automation_variables_in_environment


def delete_lcm(buckets, client_cycle):
    for bucket in buckets:
        client = next(client_cycle)
        try:
            res = client.delete_bucket_lifecycle(Bucket=bucket, ExpectedBucketOwner='admin')
            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                print('Successfully removed lcm from bucket - {}'.format(bucket))
            else:
                raise Exception
        except:
            print("unsuccessful to remove lcm from bucket - {}".format(buckets))


if __name__ == '__main__':
    setup_cluster_automation_variables_in_environment(cluster_ip="10.14.29.182", password='admin')
    client_cycle = get_client_cycle()
    buckets = get_buckets_from_prefix(next(client_cycle), 'LCMTestBucket_')
    delete_lcm(buckets, client_cycle)
