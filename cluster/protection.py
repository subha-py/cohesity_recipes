import requests
import urllib3
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import sys
sys.path.append('~/PycharmProjects/cohesity_recipes/cluster/protection.py')
sys.path.append('~/PycharmProjects/cohesity_recipes')
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import os
import time
from itertools import cycle
import random
from cluster.connection import setup_cluster_automation_variables_in_environment, get_headers
import concurrent.futures
from datetime import datetime, timedelta
import argparse

def get_bucket_info(bucket_name):
    # todo clean up headers with a method
    ips = os.environ.get("node_ips").split(",")
    cluster_ip = random.choice(ips)
    response = requests.request("GET", "https://{}/irisservices/api/v1/public/views?viewNames={}".format(cluster_ip, bucket_name), verify=False,
        headers=get_headers())
    if response.status_code == 200:
        return response.json()['views'][0]


def get_protection_info(protection_name):
    # todo clean up headers with a method
    ips = os.environ.get("node_ips").split(",")
    cluster_ip = random.choice(ips)
    headers = get_headers()
    response = requests.request("GET",
        "https://{ip}/v2/data-protect/protection-groups?names={protection_name}".format(ip=cluster_ip, protection_name=protection_name), verify=False,
        headers=headers)
    if response.status_code == 200:
        res = response.json()
        if res:
            return res['protectionGroups'][0]
        else:
            return False


def get_view_box_info(viewbox_name):
    # todo clean up headers with a method
    ips = os.environ.get("node_ips").split(",")
    cluster_ip = random.choice(ips)
    response = requests.request("GET", "https://{}/irisservices/api/v1/public/viewBoxes?names={}".format(cluster_ip, viewbox_name), verify=False,
        headers=get_headers())
    if response.status_code == 200:
        return response.json()[0]


def get_policy_info(policy_name):
    ips = os.environ.get("node_ips").split(",")
    cluster_ip = random.choice(ips)
    # todo use params
    response = requests.request("GET", "https://{}/irisservices/api/v1/public/protectionPolicies?names={}&environments=kView".format(cluster_ip, policy_name),
        verify=False, headers=get_headers())
    if response.status_code == 200:
        return response.json()[0]


def process_protection_request(bucket_name, ip, policy_id, effective_now, protection_group_prefix="s3-test-", force=False):
    try:
        bucket_info = get_bucket_info(bucket_name)
        if not force and bucket_info.get('viewProtection'):
            print("bucket: {} is already protected".format(bucket_name))
            return
        pg_name = "{}{}".format(protection_group_prefix, bucket_info['name'])
        if 'Object' not in pg_name:
            indexing_policy = {"disableIndexing": False, "allowPrefixes": ["/"]}
        else:
            indexing_policy = {"disableIndexing": True, }
        data = {"name": pg_name, "environment": "kView", "policyId": policy_id, "viewBoxId": bucket_info['viewBoxId'], "sourceIds": [bucket_info['viewId']],
            "startTime": {"hour": 23, "minute": 28}, "timezone": "Asia/Calcutta", "indexingPolicy": indexing_policy,
            "remoteViewConfigList": [{"sourceViewId": bucket_info['viewBoxId'], "useSameViewName": True}]

        }
        print("trying to create protection group - {} - for - {}".format(pg_name, bucket_name))
        response = requests.request("POST", "https://{}/irisservices/api/v1/public/protectionJobs".format(ip), verify=False, headers=get_headers(), json=data)

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
    response = requests.request("GET", "https://{}/v2/data-protect/protection-groups".format(ip), verify=False, headers=get_headers())
    return response.json()['protectionGroups']


def delete_protection_group(pg_name):
    info = get_protection_info(pg_name)
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    body = {"deleteSnapshots": True}
    response = requests.request("DELETE", "https://{}/irisservices/api/v1/public/protectionJobs/{}".format(ip, info['id']), verify=False,
        headers=get_headers(), json=body)
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
            if bucketProtection.get("jobName").startswith("_DELETED"):  # after deletion of a pg it named as DELETED_pg_name
                # todo: is this a bug investigate
                continue
            delete_protection_group(bucketProtection.get("jobName"))


def run_protection_group(protection_name=None, protection_id=None):
    if not protection_id:
        res = get_protection_info(protection_name)
        protection_id = res['id']
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    data = {"runType": "kRegular"}
    response = requests.request("POST", "https://{}/irisservices/api/v1/public/protectionJobs/run/{}".format(ip, protection_id), verify=False,
        headers=get_headers(), json=data)
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
        process_protection_request(*arg, force=force)  # pool.close()  # pool.join()


def pause_protection_job(id):
    data = {
      "action": "kPause",
      "ids": [
        id,
      ],
      "lastPauseReason": "kTenantDeactivation",
      "pausedNote": "string",
      "tenantId": "string"
    }
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    response = requests.request("POST", f"https://{ip}/v2/data-protect/protection-groups/states", verify=False,
        headers=get_headers(), json=data)
    if response.status_code == 200:
        print("Successfully paused pg - {}".format(id))
        return True
    else:
        print("pausing pg is unsuccessful - {} - on ip - {}".format(id, ip))
        return False


def cancel_pending_protection_job_runs(pgs, delete_pg=False, pause=False, delete_snapshots=False):
    def cancel_pending_runs_of_pg(pg, delete_pg, pause, delete_snapshots):
        def sleep_between(pg):
            sleep_time = random.randint(10,30)
            print(f'going to sleep for pg - {pg} for - {sleep_time}')
            time.sleep(sleep_time)
        def cancel_run(id, cancel_params):
            ips = os.environ.get("node_ips").split(",")
            ip = random.choice(ips)
            data = {'action': 'Cancel', 'cancelParams': cancel_params}
            sleep_between(id)
            response = requests.request("POST", "https://{}/v2/data-protect/protection-groups/{}/runs/actions".format(ip, id), verify=False,
                headers=get_headers(), json=data)
            if response.status_code == 202:
                print("task - data - {} is successfully cancelled - {}".format(data, pg))
            else:
                print("could not cancel running task - data - {} {}".format(data, pg))

            cancel_params = [{'runId': cancel_params[0]['runId']}]
            return cancel_params

        info = get_protection_info(pg)
        if not info:
            return
        id = info['id']
        sleep_between(pg)
        print("cancelling runs on - pg - {}".format(pg))
        if pause:
            if pause_protection_job(id):
                print('pg - {} is paused'.format(pg))
        ips = os.environ.get("node_ips").split(",")
        ip = random.choice(ips)
        response = requests.request("GET", f'https://{ip}/v2/data-protect/protection-groups/{id}/runs', verify=False, headers=get_headers())
        sleep_between(pg)
        # getting all runs
        num_runs = response.json()['totalRuns']
        dt = datetime.now() - timedelta(days=20)
        dt_s = str(dt.timestamp())
        dt_s = ''.join(dt_s.split('.'))
        response = requests.request("GET", f'https://{ip}/v2/data-protect/protection-groups/{id}/runs',
            params={'numRuns': num_runs, }, verify=False, headers=get_headers())
        if response.status_code != 200:
            print("Failed to get runs for pg - {}".format(pg))
            return
        runs = response.json()['runs']

        for run in runs:
            delete_snaps_params = [{'runId': run.get('id')}]
            delete_snapshots_for_run = delete_snapshots
            if run['isLocalSnapshotsDeleted'] == True:
                delete_snapshots_for_run = False
            cancel_params = [{}]
            cancel_params[0]['runId'] = run.get('id')
            target_statuses = ['Accepted', 'Running']
            if run.get('localBackupInfo'):
                if run.get('localBackupInfo').get('status') in target_statuses:
                    cancel_params[0]['localTaskId'] = run.get('localBackupInfo').get('localTaskId')
                    cancel_params = cancel_run(id, cancel_params)
                if delete_snapshots_for_run:
                    delete_snaps_params[0]['localSnapshotConfig'] = {"deleteSnapshot": True}
            if run.get('replicationInfo'):
                cancel_params[0]['replicationTaskId'] = []
                for replication in run['replicationInfo']['replicationTargetResults']:
                    if replication.get('status') in target_statuses:
                        cancel_params[0]['replicationTaskId'].append(replication.get('replicationTaskId'))
                        cancel_params = cancel_run(id, cancel_params)
                    if delete_snapshots_for_run:
                        if delete_snaps_params[0].get('replicationSnapshotConfig') is None:
                            delete_snaps_params[0]['replicationSnapshotConfig'] = {'updateExistingSnapshotConfig': []}
                        delete_snaps_params[0]['replicationSnapshotConfig']['updateExistingSnapshotConfig'].append(
                            {"deleteSnapshot": True,
                             "id": replication.get('clusterId'),
                             "name": replication.get('clusterName')
                            }
                        )

            if run.get('archivalInfo'):
                cancel_params[0]['archivalTaskId'] = []
                for archival in run['archivalInfo']['archivalTargetResults']:
                    if archival.get('status') in target_statuses:
                        cancel_params[0]['archivalTaskId'].append(archival.get('archivalTaskId'))
                        cancel_params = cancel_run(id, cancel_params)
                    if delete_snapshots_for_run:
                        if delete_snaps_params[0].get('archivalSnapshotConfig') is None:
                            delete_snaps_params[0]['archivalSnapshotConfig'] = {'updateExistingSnapshotConfig': []}
                        delete_snaps_params[0]['archivalSnapshotConfig']['updateExistingSnapshotConfig'].append(
                            {"deleteSnapshot": True,
                             "id": archival.get('targetId'),
                             "name": archival.get('targetName'),
                             "archivalTargetType": archival.get('targetType'),
                            }
                        )

            if delete_snapshots_for_run and len(delete_snaps_params[0]) > 1:
                sleep_between(pg)
                delete_snaps_params = {'updateProtectionGroupRunParams':delete_snaps_params}
                response = requests.request("PUT", f'https://{ip}/v2/data-protect/protection-groups/{id}/runs', verify=False, headers=get_headers(),
                json=delete_snaps_params)
                if response.status_code == 207:
                    print("snapshots successfully deleted - for run {}, data - {}".format(id, delete_snaps_params))
                else:
                    print("failed to delete snapshots - for run {}, data - {}".format(id, delete_snaps_params))

    if not pgs:
        return
    future_to_pg = {}
    thread = os.environ.get("node_ips").count(",")
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(pgs), thread)) as executor:
        for pg in pgs:
            arg = (pg, delete_pg, pause, delete_snapshots)
            future_to_pg[executor.submit(cancel_pending_runs_of_pg, *arg)] = pg
    for future in concurrent.futures.as_completed(future_to_pg):
        pg = future_to_pg[future]
        try:
            res = future.result()
        except Exception as exc:
            print("%r generated an exception: %s" % (pg, exc))
        else:
            print("protection group - {}, processed".format(pg))


def get_job_from_id(pg_list, id):
    for pg in pg_list:
        if pg['uid']['id'] == int(id):
            return pg


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Program to cancel and delete snapshots in cluster')
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')


    required.add_argument('--clusterip', help='ip/hostname of the db', type=str, required=True)
    result = parser.parse_args()
    cluster_ip = result.clusterip
    setup_cluster_automation_variables_in_environment(cluster_ip=cluster_ip, password='Syst7mt7st')
    pgs = get_all_cluster_protection_jobs()
    pg_name_list = []
    print('pgs - {}'.format(pgs))
    for pg in pgs:
            pg_name_list.append(pg['name'])
    cancel_pending_protection_job_runs(pgs=pg_name_list, delete_pg=False, pause=True, delete_snapshots=True)
