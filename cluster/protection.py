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
        res = response.json()
        if res:
            return res[0]
        else:
            return False

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
                run_protection_group(protection_id=id)
            else:
                return response_data
        else:
            print("unsuccessful to create protection group for {} - {}".format(pg_name, bucket_name))
    except Exception as ex:
        print("Could not create pg due to - {}".format(ex))

# todo replace policy id with name
def delete_all_protection_job():

    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET",
                                "https://{}/irisservices/api/v1/public/protectionRuns/".format(
                                    ip), verify=False,
                                headers=headers)

    if response.status_code == 200:
        run_ids = []
        for run in response.json():
            run_ids.append(run['jobId'])
            response = requests.request("POST",
                                        )

def run_protection_group(protection_name=None, protection_id=None):
    if not protection_id:
        res = get_protection_info(protection_name)
        protection_id = res['id']
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    data = {
        "runType": "kRegular"
    }
    response = requests.request("POST",
                                "https://{}/irisservices/api/v1/public/protectionJobs/run/{}".format(
                                    ip, protection_id), verify=False,
                                headers=headers, json=data)
    if response.status_code == 204:
        print("Successfully started run for protection group - {}".format(protection_id))
        return True
    else:
        print(response)
        return False

def run_bucket_protection(bucket_name):
    bucket_info = get_bucket_info(bucket_name)
    if bucket_info.get('viewProtection'):
        print("bucket: {} is protected".format(bucket_name))
        protection_id = bucket_info.get("viewProtection")['protectionJobs'][0].get('jobId')
        run_protection_group(protection_id=protection_id)
    else:
        print("Bucket is not protected")

def create_protection(bucket_list, policy_id=None, policy_id_list=None, effective_now=True):
    print("got buckets - {}".format(len(bucket_list)))
    ips = os.environ.get("node_ips").split(",")
    ip_cycle = cycle(ips)
    # pool = Pool(processes=cpu_count() - 1)
    for bucket_name in bucket_list:
        if policy_id_list is not None:
            policy_id = random.choice(policy_id_list)
        if 'Object' in bucket_name:
            policy_id = policy_id_list[0]
        arg = (bucket_name, next(ip_cycle), policy_id, effective_now, "subha_")
        # process_protection_request(*arg)
        # pool.apply_async(process_protection_request, args=arg)
        process_protection_request(*arg)
    # pool.close()
    # pool.join()

if __name__ == '__main__':
    setup_cluster_automation_variables_in_environment(cluster_ip="10.14.29.182",password="admin")
    # pg_res = get_protection_info('subha-sample-pg')
    # print(pg_res)
    #
    # bucket_res = get_bucket_info("LCMTestBucket_Short_19")
    # print(bucket_res)
    #
    # viewbox_res = get_view_box_info("NFS_stress_VB")
    # print(viewbox_res)
    # client_cycle = get_client_cycle()
    # policy_id_list = []
    # policy_id_list.append(get_policy_info('sbera_IF_policy').get('id'))
    # policy_id_list.append(get_policy_info('sbera_IPF_policy').get('id'))
    # buckets = get_buckets_from_prefix(next(client_cycle), prefix="LCMTestBucket_Random")
    # for bucket in buckets:
    #     run_bucket_protection(bucket)
    types = ["Random", "Short", "Hierarchical", "Long", "Object"]
    for type in types:
        for i in range(20):
            pg = "sb_LCMTestBucket_{}_{}".format(type, i)
            print("working on - pg - {}".format(pg))
            info = get_protection_info(pg)
            if not info:
                continue
            id = info['id']

            ips = os.environ.get("node_ips").split(",")
            ip = random.choice(ips)
            headers = {'Content-Type': "application/json", 'accept': "application/json"}
            headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
            response = requests.request("GET",
                                        "https://{}/irisservices/api/v1/public/protectionRuns?jobId={}".format(
                                            ip, id), verify=False,
                                        headers=headers)
            runs = response.json()
            for run in runs:
                copyRuns = run.get('copyRun')
                if copyRuns:
                    for copyRun in copyRuns:
                       if copyRun["target"].get("type") == "kRemote":
                           if copyRun.get('status') == 'kRunning' or copyRun.get('status') == 'kAccepted':
                                data = {"copyTaskUid":copyRun.get("taskUid")}
                                ips = os.environ.get("node_ips").split(",")
                                ip = random.choice(ips)
                                response = requests.request("POST",
                                                            "https://{}/irisservices/api/v1/public/protectionRuns/cancel/{}".format(ip, id),
                                                            verify=False,
                                                            headers=headers, json=data)
                                if response.status_code == 204:
                                    print("task - data - {} is successfully cancelled - {}".format(data, pg))

                                else:
                                    print("could not cancel running task - data - {} {}".format(data, pg))

    # todo create methods for these