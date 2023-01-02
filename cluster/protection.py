import sys
sys.path.extend(['/home/cohesity/PycharmProjects/cohesity_recipes'])

from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
import urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import sys
import os
import time
from itertools import cycle
import random
from cluster.connection import setup_cluster_automation_variables_in_environment, get_client_cycle
from multiprocessing import Pool, cpu_count
from s3.utils.bucket import (
    get_buckets_from_prefix,
)
import concurrent.futures

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

def process_protection_request(bucket_name, ip, policy_id, effective_now, protection_group_prefix = "s3-test-",
                               force=False):
    try:
        headers = {'Content-Type': "application/json", 'accept': "application/json"}
        headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
        bucket_info = get_bucket_info(bucket_name)
        if not force and bucket_info.get('viewProtection'):
            print("bucket: {} is already protected".format(bucket_name))
            return
        pg_name = "{}{}".format(protection_group_prefix ,bucket_info['name'])
        if 'Object' not in pg_name:
            indexing_policy = {
                  "disableIndexing": False,
                  "allowPrefixes": [
                    "/"
                  ]
                }
        else:
            indexing_policy = {
                "disableIndexing": True,
            }
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
            "indexingPolicy": indexing_policy,
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

def get_all_cluster_protection_jobs():
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET",
                                "https://{}/irisservices/api/v1/public/protectionJobs".format(
                                    ip), verify=False,
                                headers=headers)
    return response.json()
def delete_protection_group(pg_name):
    info = get_protection_info(pg_name)
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    body = {
        "deleteSnapshots": True
    }
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("DELETE",
                                "https://{}/irisservices/api/v1/public/protectionJobs/{}".format(
                                    ip, info['id']), verify=False,
                                headers=headers, json=body)
    if response.status_code == 204:
        print("deletion of protection group - {} is successful".format(pg_name))
    else:
        print("deletion of protection group - {} is unsuccessful".format(pg_name))
def unprotect_all_buckets(buckets):
    for bucket in buckets:
        print("working on bucket - {}".format(bucket))
        bucket_info = get_bucket_info(bucket)
        if not bucket_info.get('viewProtection'):
            continue
        bucketProtections = bucket_info.get('viewProtection')['protectionJobs']
        ips = os.environ.get("node_ips").split(",")
        ip = random.choice(ips)
        for bucketProtection in bucketProtections:
            if bucketProtection.get("jobName").startswith("_DELETED"): # after deletion of a pg it named as DELETED_pg_name
                # todo: is this a bug investigate
                continue
            delete_protection_group(bucketProtection.get("jobName"))
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

def create_protection(bucket_list, policy_id=None, policy_id_list=None, effective_now=True, force=False, prefix="subha_"):
    print("got buckets - {}".format(len(bucket_list)))
    ips = os.environ.get("node_ips").split(",")
    ip_cycle = cycle(ips)
    # pool = Pool(processes=cpu_count() - 1)
    for bucket_name in bucket_list:
        if policy_id_list is not None:
            policy_id = random.choice(policy_id_list)
        arg = (bucket_name, next(ip_cycle), policy_id, effective_now, prefix)
        process_protection_request(*arg, force=force)
    # pool.close()
    # pool.join()

def pause_protection_job(pg_name):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    info = get_protection_info(pg_name)
    if not info:
        return
    data = {
          "pause": True,
          "pauseReason": 0
        }
    response = requests.request("POST",
                                "https://{}/irisservices/api/v1/public/protectionJobState/{}".format(
                                    ip, info['id']),
                                verify=False,
                                headers=headers, json=data)
    if response.status_code == 204:
        print("Successfully paused pg - {}".format(pg_name))
    else:
        print("pausing pg is unsuccessful - {}".format(pg_name))

def cancel_pending_protection_job_runs(pgs, delete_pg=False, pause=False, thread_num=None):
    def cancel_pending_runs_of_pg(pg, delete_pg, pause):
        if pause:
            pause_protection_job(pg)
        print("cancelling runs on - pg - {}".format(pg))
        info = get_protection_info(pg)
        if not info:
            return
        id = info['id']
        ips = os.environ.get("node_ips").split(",")
        ip = random.choice(ips)
        headers = {'Content-Type': "application/json", 'accept': "application/json"}
        headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
        response = requests.request("GET",
                                    "https://{}/irisservices/api/v1/public/protectionRuns?jobId={}".format(
                                        ip, id), verify=False,
                                    headers=headers)

        if response.status_code != 200:
            print("Failed to get runs for pg - {}".format(pg))
            return
        runs = response.json()
        for run in runs:
            copyRuns = run.get('copyRun')
            if copyRuns:
                for copyRun in copyRuns:
                    target = copyRun["target"].get("type")
                    if target == "kRemote" or target == "kArchival" or target == "kLocal":
                        if copyRun.get('status') == 'kRunning' or copyRun.get('status') == 'kAccepted':
                            data = {"copyTaskUid": copyRun.get("taskUid")}
                            ips = os.environ.get("node_ips").split(",")
                            ip = random.choice(ips)
                            response = requests.request("POST",
                                                        "https://{}/irisservices/api/v1/public/protectionRuns/cancel/{}".format(
                                                            ip, id),
                                                        verify=False,
                                                        headers=headers, json=data)
                            if response.status_code == 204:
                                print("task - data - {} is successfully cancelled - {}".format(data, pg))

                            else:
                                print("could not cancel running task - data - {} {}".format(data, pg))
        if delete_pg:
            time.sleep(5)
            delete_protection_group(pg)

    if not pgs:
        return
    future_to_pg = {}
    thread = os.environ.get("node_ips").count(",")
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(pgs), thread)) as executor:
        for pg in pgs:
            arg = (pg,delete_pg, pause)
            future_to_pg[executor.submit(cancel_pending_runs_of_pg, *arg)] = pg
    for future in concurrent.futures.as_completed(future_to_pg):
        pg = future_to_pg[future]
        try:
            res = future.result()
        except Exception as exc:
            print("%r generated an exception: %s" % (pg, exc))
        else:
            print("deleted protection group - {}".format(pg))

if __name__ == '__main__':
    setup_cluster_automation_variables_in_environment(cluster_ip="10.14.29.182",password='admin')
    # pause_protection_job('subha_LCMTestBucket_Object_1')
    pgs = get_all_cluster_protection_jobs()
    pg_name_list = []
    for pg in pgs:
        if "LCM" in pg['name']:
            pg_name_list.append(pg['name'])
    cancel_pending_protection_job_runs(pgs=pg_name_list, delete_pg=False, pause=False)