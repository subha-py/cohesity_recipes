from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
import urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


import os
from itertools import cycle
import random
from cluster.connection import setup_cluster_automation_variables_in_environment, get_client_cycle
from multiprocessing import Pool, cpu_count
from s3.utils.bucket import (
    get_buckets_from_prefix,
)

def get_bucket_info(bucket_name):
    # todo clean up headers with a method
    ips = os.environ.get("node_ips").split(",")
    cluster_ip = random.choice(ips)
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET",
                                "https://{}/irisservices/api/v1/public/views?viewNames={}".format(cluster_ip,
                                                                                                  bucket_name),
                                verify=False, headers=headers)
    if response.status_code == 200:
        return response.json()['views'][0]


def get_policy_info(policy_name):
    ips = os.environ.get("node_ips").split(",")
    cluster_ip = random.choice(ips)
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    # todo use params
    response = requests.request("GET",
                                "https://{}/irisservices/api/v1/public/protectionPolicies?names={}&environments=kView".format(
                                    cluster_ip, policy_name),
                                verify=False, headers=headers)
    if response.status_code == 200:
        return response.json()[0]

def process_protection_request(bucket_name, ip, policy_id, effective_now):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    bucket_info = get_bucket_info(bucket_name)
    if bucket_info.get('viewProtection'):
        print("bucket: {} is already protected".format(bucket_name))
        return
    data = {
        "name": "s3-test-{}".format(bucket_info['name']),
        "environment": "kView",
        "policyId": policy_id,
        "viewBoxId": bucket_info['viewBoxId'],
        "parentSourceId": 1,
        "sourceIds": [
            bucket_info['viewId']
        ],
        "timezone": "Asia/Calcutta"
    }
    response = requests.request("POST",
                                "https://{}/irisservices/api/v1/public/protectionJobs".format(ip),
                                verify=False,
                                headers=headers, json=data)
    if response.status_code == 201:
        response_data = response.json()
        if effective_now:
            id = response_data["uid"]["id"]
            data = {
                "runType": "kRegular"
            }
            response = requests.request("POST",
                                        "https://{}/irisservices/api/v1/public/protectionJobs/run/{}".format(
                                            ip, id), verify=False,
                                        headers=headers, json=data)
            if response.status_code == 204:
                print("Successfully protected bucket - {}".format(bucket_name))
                return "Success"
            else:
                return "Failed"
        else:
            return response_data

# todo replace policy id with name
def create_protection(bucket_list, policy_id="3928833934229838:1668777508219:118686", effective_now=True):

    ips = os.environ.get("node_ips").split(",")
    ip_cycle = cycle(ips)
    # pool = Pool(processes=cpu_count() - 1)
    for bucket_name in bucket_list:
        arg = (bucket_name, next(ip_cycle), policy_id, effective_now)
        process_protection_request(*arg)
        # pool.apply_async(process_protection_request, args=arg)
    # pool.close()
    # pool.join()

if __name__ == '__main__':
    setup_cluster_automation_variables_in_environment(cluster_ip="10.2.195.75")
    client_cycle = get_client_cycle()

    buckets = get_buckets_from_prefix(next(client_cycle), prefix="LCMTestBucket_Object_0")
    res = create_protection(buckets)