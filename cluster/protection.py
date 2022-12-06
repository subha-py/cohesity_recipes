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

def get_protection_info(protection_name):
    # todo clean up headers with a method
    ips = os.environ.get("node_ips").split(",")
    cluster_ip = random.choice(ips)
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET",
                                "https://{}/irisservices/api/v1/public/protectionJobs?names={}".format(cluster_ip,
                                                                                                  protection_name),
                                verify=False, headers=headers)
    if response.status_code == 200:
        return response.json()[0]

def get_view_box_info(viewbox_name):
    # todo clean up headers with a method
    ips = os.environ.get("node_ips").split(",")
    cluster_ip = random.choice(ips)
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET",
                                "https://{}/irisservices/api/v1/public/viewBoxes?names={}".format(cluster_ip,
                                                                                                  viewbox_name),
                                verify=False, headers=headers)
    if response.status_code == 200:
        return response.json()[0]
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

def process_protection_request(bucket_name, ip, policy_id, effective_now, protection_group_prefix = "s3-test-"):
    try:
        headers = {'Content-Type': "application/json", 'accept': "application/json"}
        headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
        bucket_info = get_bucket_info(bucket_name)
        if bucket_info.get('viewProtection'):
            print("bucket: {} is already protected".format(bucket_name))
            return
        pg_name = "{}{}".format(protection_group_prefix ,bucket_info['name'])
        data = {
            "name": pg_name,
            "environment": "kView",
            "policyId": policy_id,
            "viewBoxId": bucket_info['viewBoxId'],
            "sourceIds": [
                bucket_info['viewId']
            ],
            "startTime":{
              "hour": 23,
              "minute": 28
            },
            "timezone": "Asia/Calcutta",
            "indexingPolicy": {
              "disableIndexing": False,
              "allowPrefixes": [
                "/"
              ]
            },
            "remoteViewConfigList": [
              {
                "sourceViewId": bucket_info['viewBoxId'],
                "useSameViewName": True
              }
            ]

        }
        print("trying to create protection group - {} - for - {}".format(pg_name, bucket_name))
        response = requests.request("POST",
                                    "https://{}/irisservices/api/v1/public/protectionJobs".format(ip),
                                    verify=False,
                                    headers=headers, json=data)

        if response.status_code == 201:
            print("successfully protection group for {}- {}".format(pg_name, bucket_name))
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
                    print("Successfully started run for protected bucket - {}".format(bucket_name))
                    return "Success"
                else:
                    return "Failed"
            else:
                return response_data
        else:
            print("unsuccessful to create protection group for {} - {}".format(pg_name, bucket_name))
    except Exception as ex:
        print("Could not create pg due to - {}".format(ex))

# todo replace policy id with name
def create_protection(bucket_list, policy_id, effective_now=True):
    print("got buckets - {}".format(len(bucket_list)))
    ips = os.environ.get("node_ips").split(",")
    ip_cycle = cycle(ips)
    # pool = Pool(processes=cpu_count() - 1)
    for bucket_name in bucket_list:
        arg = (bucket_name, next(ip_cycle), policy_id, effective_now, "subha_")
        # process_protection_request(*arg)
        # pool.apply_async(process_protection_request, args=arg)
        process_protection_request(*arg)
    # pool.close()
    # pool.join()

if __name__ == '__main__':
    setup_cluster_automation_variables_in_environment(cluster_ip="10.2.200.155")
    # pg_res = get_protection_info('subha-sample-pg')
    # print(pg_res)
    #
    # bucket_res = get_bucket_info("LCMTestBucket_Short_19")
    # print(bucket_res)
    #
    # viewbox_res = get_view_box_info("NFS_stress_VB")
    # print(viewbox_res)
    client_cycle = get_client_cycle()
    #
    buckets = get_buckets_from_prefix(next(client_cycle), prefix="LCMTestBucket_")
    res = create_protection(buckets, policy_id="6435825238425154:1650481288823:8214275")