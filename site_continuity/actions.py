import uuid
import os
import requests
import concurrent.futures
import time
import random

from site_continuity.connection import get_base_url, get_headers, set_environ_variables
from site_continuity.applications import get_replicated_snapshots
from site_continuity.dr_plans import get_dr_plans

from cluster.connection import setup_cluster_automation_variables_in_environment

def activate(name=None, dr_plan_info=None):
    if not  dr_plan_info:
        dr_plan_info = get_dr_plans(name)[0]
    data = {'action': 'Activate'}
    response = requests.request("POST", "{base_url}/dr-plans/{plan_id}/actions".format(base_url=get_base_url(ip),
                                                                                       plan_id=dr_plan_info.get('id')),
                                verify=False,
                                headers=get_headers(), json=data)
    if response.status_code == 201:
        print('{name} is successfully activated'.format(name=dr_plan_info.get('name')))
    else:
        response = response.json()
        print('unsuccessful to activate {name}, due to error - {error}'.format(name=dr_plan_info.get('name'),
                                                                               error=response.get('errorMessage')))
def prepare_for_failback(name=None, dr_plan_info=None):
    if not dr_plan_info:
        dr_plan_info = get_dr_plans(name)[0]
    data = {'action': 'PrepareForFailback'}
    response = requests.request("POST", "{base_url}/dr-plans/{plan_id}/actions".format(base_url=get_base_url(ip),
                                                                                       plan_id=dr_plan_info.get('id')),
                                verify=False,
                                headers=get_headers(), json=data)
    if response.status_code == 201:
        print('{name} is successfully prepared for failback'.format(name=dr_plan_info.get('name')))
    else:
        response = response.json()
        print('unsuccessful to prepared for failback {name}, due to error - {error}'.format(name=dr_plan_info.get('name'),
                                                                               error=response.get('errorMessage')))


def failover(dr_plan_info=None, dr_plan_name=None, performStorageVmotion=False, test_failover=False):

    if not dr_plan_info:
        dr_plan_info = get_dr_plans(name=dr_plan_name)[0]
    refresh_replicated_snapshots(dr_id=dr_plan_info.get('id'))
    vms = get_replicated_snapshots(dr_plan_info.get('appId'))
    objectSnapshotOverrides = []
    for vm in vms:
        snap = {'objectId': vm.get('objectDetails').get('objectId'),
                'snapshotId': vm.get('replicatedSnapshots')[0].get('snapshotId')}
        objectSnapshotOverrides.append(snap)
    if test_failover:
        action = 'TestFailover'
        actionKey = 'testFailoverParams'
        vmwareParams = {
            "performStorageVmotion": performStorageVmotion,
            "resourceProfileName":
                dr_plan_info['drSite']['source']['vmwareParams']['vCenterParams']['resourceProfiles'][0]['name']
        }
    else:
        action = 'Failover'
        actionKey = 'failoverParams'
        vmwareParams = {
            "shutdownVms": False,
            "protectVms": True,
            "resourceProfileName":
                dr_plan_info['drSite']['source']['vmwareParams']['vCenterParams']['resourceProfiles'][0]['name']
        }
    data = {'action': action,
            actionKey:{
                "objectSnapshotOverrides": objectSnapshotOverrides,
                "environment": "vmware",
                "vmwareParams": vmwareParams
                }
            }

    response = requests.request("POST", "{base_url}/dr-plans/{plan_id}/actions".format(base_url=get_base_url(ip),
                                                                                       plan_id=dr_plan_info.get('id')),
                                verify=False,
                                headers=get_headers(), json=data)
    if response.status_code == 201:
        if test_failover:
            print('test failover for {name} is successfully triggered'.format(name=dr_plan_info.get('name')))
        else:
            print('failover for {name} is successfully triggered'.format(name=dr_plan_info.get('name')))
    else:
        response = response.json()
        print('unsuccessful to test failover for {name}, due to error - {error}'.format(name=dr_plan_info.get('name'),
                                                                               error=response.get('errorMessage')))

def failover_in_parallel(plans, **kwargs):
    future_to_plan = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(plans)) as executor:
        for plan in plans:
            print('working on plan - {}'.format(plan.get("id")))
            kwargs['dr_plan_info'] = plan
            future_to_plan[executor.submit(failover,**kwargs)] = plan.get("id")
        for future in concurrent.futures.as_completed(future_to_plan):
            plan_id = future_to_plan[future]
            try:
                res = future.result()
            except Exception as exc:
                print("%r generated an exception: %s" % (plan_id,exc))
            else:
                if kwargs.get('test_failover'):
                    print("test failover is initiated - {}".format(plan_id))
                print("failover is initiated - {}".format(plan_id))

def get_actions(dr_plan_id):
    ip = os.environ.get('ip')
    params = {'drPlanIds': dr_plan_id}
    response = requests.request("GET", "{base_url}/actions".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), params=params)
    if response.status_code == 200:
        actions =  response.json()['actions']
        actions.sort(key=lambda x: x['attempts'][0]['endTimeUsecs'], reverse=True)
        return actions
    else:
        response = response.json()
        print('unsuccessful to get actions for dr_plan for {name}, due to error - {error}'.format(name=dr_plan_id,
                                                                                        error=response.get(
                                                                                            'errorMessage')))
def teardown(dr_plan_id, action_to_tear='TestFailover'):
    actions = get_actions(dr_plan_id)
    ip = os.environ.get('ip')
    for action in actions:
        if action.get('type') == action_to_tear and \
                (action.get('status') == 'Completed' or action.get('status') == 'Failed'):
            data = {"action":"Teardown",
                    "teardownParams":{"actionId":action.get('id')}}
            response = requests.request("POST", "{base_url}/dr-plans/{dr_id}/actions".format(base_url=get_base_url(ip),
                                                                                    dr_id=dr_plan_id), verify=False,
                                headers=get_headers(), json=data)

            if response.status_code == 201:
                print('Teardown triggered successfully for - {}'.format(dr_plan_id))
                return response.json()
            else:
                response = response.json()
                print('unsuccessful to teardown for dr_plan for {name}, due to error - {error}'.format(
                    name=dr_plan_id, error=response.get('errorMessage')))
                return response
    print('Could not find action to teardown for dr-id - {}'.format(dr_plan_id))

def refresh_replicated_snapshots(dr_id):
    data = {
        "operationId": str(uuid.uuid4()),
        "operationType": "RefreshReplicatedSnapshots",
        "refreshReplicatedSnapshotsParams": {
            "drPlanId": dr_id
        }
    }
    ip = os.environ.get('ip')
    response = requests.request("POST", "{base_url}/objects/operations".format(base_url=get_base_url(ip)),
                                verify=False,
                                headers=get_headers(), json=data)

    if response.status_code == 201:
        status = 'InProgress'
        counter = 0
        # will wait for 20 mins
        while status!= 'Success' and counter < 60:
            print('going to sleep for 20 secs before polling for refresh snapshot status - {dr_id}, counter = {counter}'
                  .format(dr_id=dr_id,counter=counter))
            time.sleep(20)
            # wait for refresh to complete
            wait_response = requests.request("GET", "{base_url}/objects/operations/{op_id}".format(
                base_url=get_base_url(ip), op_id=data['operationId']), verify=False,
                                    headers=get_headers())
            if wait_response.status_code == 200:
                wait_response = wait_response.json()
                status = wait_response.get('status')
                counter += 1
            else:
                wait_response = wait_response.json()
                print("Unsuccessful to get status of refresh snapshot action for dr_plan named - {dr_id} due to error- \
                      {error}".format(
                    dr_id=dr_id,
                    error=wait_response.get('errorMessage')
                ))
                return wait_response
        if counter >= 60:
            raise SystemError("Unsuccessful to get status of refresh snapshot action for dr_plan named - {dr_id} due to error- \
                                  {error}".format(
                dr_id=dr_id,
                error='waited for 10mins refresh is not completed'
            ))
        print('Refresh Snapshots is successful')
        return response.json()
    else:
        response = response.json()
        print("Unsuccessful to refresh snapshots for dr_plan named - {dr_id} due to error - {error}".format(
            dr_id=dr_id,
            error=response.get('errorMessage')
        ))
        return response

if __name__ == '__main__':
    ip = 'helios-sandbox.cohesity.com'
    set_environ_variables({'ip': ip})
    setup_cluster_automation_variables_in_environment('10.14.7.5')

    # # profile 1 #dhcp 3vms
    # create profile 1
    # apps = get_applications('profile_1')
    # for app in apps:
    #     app_name = app.get('name')
    #     res = create_dr_plan(name="phase_1_{}-dr_plan".format(app_name), app_info=app,source_vc='system-test-vc03.qa01.eng.cohesity.com',
    #                    dest_vc='system-test-vc01.qa01.eng.cohesity.com',dr_type='dhcp')

    # delete all profile 1 dr plans
    # dr_plans = get_dr_plans('profile_1')[:1]
    # for dr_plan in dr_plans:
    #     delete_dr_plan(dr_id=dr_plan.get('id'))

    # # profile 2 #dhcp 3vms with script + 3vms regular
    # create profile 2
    # apps = get_applications('profile_2')
    # for app in apps:
    #     app_name = app.get('name')
    #     status_code = create_dr_plan(name="{}-dr_plan".format(app_name), app_info=app,source_vc='system-test-vc02.qa01.eng.cohesity.com',
    #                    dest_vc='system-test-vc03.qa01.eng.cohesity.com',dr_type='dhcp')
    # dr_plans  = get_dr_plans(name='profile_2')
    # for plan in dr_plans:
    #     activate(dr_plan_info=plan)
    # delete all profile_2 plans
    # plans = get_dr_plans(name='profile_2')
    # for plan in plans:
    #     if plan.get('name') == 'profile_2_app_103-dr_plan':
    #         continue
    #     delete_dr_plan(dr_id=plan.get('id'))
    #
    # plans = get_dr_plans(name='profile_2_app_103-dr_plan')[0]
    # print(plans)
    # for plan in plans:
    #     if "profile_2_app_103-dr_plan" in plan.get('name'):
    #         continue
    #     delete_dr_plan(dr_id=plan.get('id'))

    # profile 3 #static
    # create all profile_3 plans
    # apps = get_applications('profile_3')
    # for app in apps:
    #     app_name = app.get('name')
    #     create_dr_plan(name="{}-dr_plan".format(app_name), app_info=app,source_vc='10.14.22.105',
    #                    dest_vc='system-test-vc02.qa01.eng.cohesity.com',dr_type='static')

    # delete all profile_3 plans
    # plans = get_dr_plans(name='profile_3')
    # for plan in plans:
    #     delete_dr_plan(dr_id=plan.get('id'))

    # delete all cdp dr plans
    # dr_plans = get_dr_plans('profile_cdp')
    # for dr_plan in dr_plans:
    #     delete_dr_plan(dr_id=dr_plan.get('id'))

    # dr_plans = get_dr_plans('profile_2')
    # random_plans = random.sample(dr_plans, 7)
    # dr_plans = get_dr_plans('profile_3')
    # random_plans += random.sample(dr_plans, 12)
    # dr_plans = get_dr_plans('profile_cdp')
    # random_plans += random.sample(dr_plans, 5)
    # result = {}
    # plan_names = []
    # for plan in random_plans:
    #     print(plan.get('name'))
    #     if plan.get('name').startswith('profile_2'):
    #         result['profile_2'] = result.get('profile_2', 0) + 1
    #     elif plan.get('name').startswith('profile_3'):
    #         result['profile_3'] = result.get('profile_3', 0) + 1
    #     elif plan.get('name').startswith('profile_cdp'):
    #         result['profile_cdp'] = result.get('profile_cdp', 0) + 1
    #     plan_names.append(plan.get('name'))
    # failover_in_parallel(plans=random_plans)
    # #
    # print(result)
    # print(plan_names)

    # app_info = get_applications('profile_3_app_31')[0]
    # res = get_replicated_snapshots(app_id=app_info.get('id'))
    # print(res)
    plans = ['profile_2_app_277-dr_plan', 'profile_2_app_73-dr_plan', 'profile_2_app_253-dr_plan', 'profile_2_app_175-dr_plan', 'profile_2_app_169-dr_plan', 'profile_2_app_163-dr_plan', 'profile_2_app_271-dr_plan', 'profile_3_app_28-dr_plan', 'profile_3_app_7-dr_plan', 'profile_3_app_43-dr_plan', 'profile_3_app_31-dr_plan', 'profile_3_app_70-dr_plan', 'profile_3_app_46-dr_plan', 'profile_3_app_22-dr_plan', 'profile_3_app_4-dr_plan', 'profile_3_app_1-dr_plan', 'profile_3_app_58-dr_plan', 'profile_3_app_73-dr_plan', 'profile_3_app_16-dr_plan', 'profile_cdp_2_app_2-dr_plan', 'profile_cdp_1_app_1-dr_plan', 'profile_cdp_2_app_4-dr_plan', 'profile_cdp_1_app_3-dr_plan', 'profile_cdp_1_app_2-dr_plan']
    print(len(plans))
    for plan in plans:
        prepare_for_failback(name=plan)