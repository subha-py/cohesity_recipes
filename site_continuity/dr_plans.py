import os
import time
import requests
from site_continuity.connection import get_base_url, get_headers, set_environ_variables
from site_continuity.sites import get_sites
from site_continuity.applications import get_applications, get_replicated_snapshots
from cluster.connection import \
    (get_base_url as cohesity_base_url,
     get_headers as get_cohesity_headers,
     setup_cluster_automation_variables_in_environment
     )
from vmware.connection import find_by_moid
from cluster.virtual_machines import get_vc_id
import random
def get_dr_plans(name=None):
    ip = os.environ.get('ip')
    params = None
    if name is not None:
        params = {'names': name}
    response = requests.request("GET", "{base_url}/dr-plans".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), params=params)
    if response.status_code == 200:
        response = response.json()
        return response.get('drPlans')
    else:
        print("Unsuccessful to get dr plan info - {}".format(response.status_code))
        return None

def get_dhcp_vmware_params(dr_plan_name, dest_vc):
    adict =  {'sourceType': 'vCenter',
                                    'vCenterParams': {
                                        'objectId': get_vc_id(dest_vc),
                                        'resourceProfiles': [
                                            {
                                                'defaultResourceSet': {
                                                          'computeConfig': {
                                                              'clusterId': 27873,
                                                              'clusterMoRef': 'domain-c158973',
                                                              'dataCenterId': 8,
                                                              'dataCenterMoRef': 'datacenter-18',
                                                              'dataStoreId': 28040,
                                                              'dataStoreMoRef': 'datastore-161101',
                                                              'networkPortGroupId': 15591,
                                                              'networkPortGroupMoRef': 'network-9886',
                                                              'resourcePoolId': 27874,
                                                              'resourcePoolMoRef': 'resgroup-158974'},
                                                          'name': 'Default resource set'},
                                                'ipConfig': {
                                                    'configurationType': 'DHCP',
                                                    'dhcpConfig': {
                                                        'dnsServers': [
                                                            '10.18.32.145'],
                                                        'dnsSuffixes': [
                                                            'eng.cohesity.com']}},
                                                'name': '{name}-profile'.format(name=dr_plan_name)
                                                }
                                            ]
                                        }
                                    }
    return adict


def build_manual_object_level_config(app_info, source_vc):
    result = []
    virtual_machines = app_info.get('latestAppVersion').get('spec').get('components')[0].get('objectParams')
    for vm in virtual_machines:
        vm_id = vm.get('id')
        response = requests.request('GET',"{base_url}/protectionSources/objects/{vm_id}".format(
            base_url=cohesity_base_url(),
            vm_id = vm_id
        ), headers=get_cohesity_headers(), verify=False)
        if response.status_code == 200:
            vm_moid = response.json().get('vmWareProtectionSource').get('id').get('morItem')
            system_info = {'host': source_vc}
            vmref = find_by_moid(system_info, vm_moid)
            ip_addr = vmref.guest.ipAddress
            if not ip_addr:
                raise AttributeError("Vm - {} doesn't have any ip address".format(vmref.name))
        else:
            raise AttributeError('Could not find vm_moid for vm - {}'.format(vm_id))
        adict = {
            'gateway': '10.14.16.1',
            'ipAddress': ip_addr,
            'objectId': vm_id,
            'subnet': '255.255.240.0',
            'dnsServers': [
                '10.18.32.145'],
            'dnsSuffixes': [
                'eng.cohesity.com']}
        result.append(adict)
    return result
def get_static_vmware_params(app_info, source_vc, dest_vc):
    adict = {
            'sourceType': 'vCenter',
            'vCenterParams': {
                'objectId': get_vc_id(dest_vc),
                'resourceProfiles': [{
                                     'customResourceSets': [],
                    'defaultResourceSet': {'computeConfig': {'clusterId': 17710, 'clusterMoRef': 'domain-c9202', 'dataCenterId': 16207, 'dataCenterMoRef': 'datacenter-8647', 'dataStoreId': 28033, 'dataStoreMoRef': 'datastore-12490', 'networkPortGroupId': 17480, 'networkPortGroupMoRef': 'network-8660', 'resourcePoolId': 17711, 'resourcePoolMoRef': 'resgroup-9203'}, 'name': 'Default resource set'},
                    'ipConfig': {
                                         'configurationType': 'Static',
                                         'staticConfig': {
                                             'configOption': 'Manual',
                                             'manualIpConfig': {
                                                 'manualObjectLevelConfig': build_manual_object_level_config(app_info, source_vc=source_vc)},
                                             'networkMappingConfig': None}},
                                     'name': 'static_res'}]}}
    return adict
def create_dr_plan(name, app_info,source_vc, dest_vc, primary_site="st-site-con-tx", secondary_site="st-site-con-rx",
                    description=None, rpo=None, dr_type='dhcp', activate_dr=False):
    primary_site_info = get_sites(primary_site)[0]
    secondary_site_info = get_sites(secondary_site)[0]
    if not description:
        description = name
    if not rpo:
        rpo = {'frequency': 6, 'unit': 'Hours'}
    app_id = app_info.get('id')
    data = {
        "name": name,
        "description": description,
        "primarySite": {
                'siteId': primary_site_info.get('id'),
                'source': {
                    'environment': 'vmware',
                    'vmwareParams': {'sourceType': 'vCenter',
                                    'vCenterParams': {
                                        'objectId': get_vc_id(source_vc, first=True),
                                        'resourceProfiles': []
                                        }
                                    }
                }
        },
        "drSite": {
                'siteId': secondary_site_info.get('id'),
                'source': {
                    'environment': 'vmware',
                }
        },
        "rpo": rpo,
        "appId": app_id
    }
    if dr_type == 'dhcp':
        data['drSite']['source']['vmwareParams'] = get_dhcp_vmware_params(name,dest_vc)
    elif dr_type == 'static':
        data['drSite']['source']['vmwareParams'] = get_static_vmware_params(app_info, source_vc=source_vc, dest_vc=dest_vc) # source vc is required to get the ips of vms
    response = requests.request("POST", "{base_url}/dr-plans".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), json=data)
    status_code = response.status_code
    if response.status_code == 201:
        print("Successfully created dr_plan named - {dr_name}".format(
            dr_name=name
        ))
        if activate_dr:
            time.sleep(30)
            dr_info = response.json()
            activate(dr_plan_info=dr_info)
        return status_code
    else:
        response = response.json()
        print("Unsuccessful to create dr_plan named - {dr_name} due to error - {error}".format(
            dr_name=name,
            error=response.get('errorMessage')
        ))
        return status_code

def delete_dr_plan(dr_plan_name=None, dr_id=None):
    if not dr_id:
        dr_id = get_dr_plans(dr_plan_name)[0]
    response = requests.request("DELETE", "{base_url}/dr-plans/{dr_id}".format(base_url=get_base_url(ip),
                                dr_id=dr_id),
                                verify=False,
                                headers=get_headers())
    if response.status_code == 204:
        print("dr_plan - {} is successfully deleted".format(dr_id))
    else:
        response = response.json()
        print("Unsuccessful to delete dr_plan named - {dr_id} due to error - {error}".format(
            dr_id=dr_id,
            error=response.get('errorMessage')
        ))
        return response

def delete_all():
    dr_plans = get_dr_plans()
    for dr_plan in dr_plans:
        delete_dr_plan(dr_plan.get('name'))

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
        print('unsuccessful to activate {name}, due to error - {error}'.format(name=dr_plan_info.get('name'),                                                                    error=response.get('errorMessage')))


def test_failover(dr_plan_info=None, dr_plan_name=None, performStorageVmotion=False):
    if not dr_plan_info:
        dr_plan_info = get_dr_plans(name=dr_plan_name)[0]
    vms = get_replicated_snapshots(dr_plan_info.get('appId'))
    objectSnapshotOverrides = []
    for vm in vms:
        snap = {'objectId': vm.get('objectDetails').get('objectId'),
                'snapshotId': vm.get('replicatedSnapshots')[0].get('snapshotId')}
        objectSnapshotOverrides.append(snap)
    data = {'action': 'TestFailover',
            'testFailoverParams':{
                "objectSnapshotOverrides": objectSnapshotOverrides,
                "environment": "vmware",
                "vmwareParams": {
                    "performStorageVmotion": performStorageVmotion,
                    "resourceProfileName": dr_plan_info['drSite']['source']['vmwareParams']['vCenterParams']['resourceProfiles'][0]['name']
                }
            }
            }
    response = requests.request("POST", "{base_url}/dr-plans/{plan_id}/actions".format(base_url=get_base_url(ip),
                                                                                       plan_id=dr_plan_info.get('id')),
                                verify=False,
                                headers=get_headers(), json=data)
    if response.status_code == 201:
        print('test failover for {name} is successfully triggered'.format(name=dr_plan_info.get('name')))
    else:
        response = response.json()
        print('unsuccessful to test failover for {name}, due to error - {error}'.format(name=dr_plan_info.get('name'),
                                                                               error=response.get('errorMessage')))

def get_actions(dr_plan_id):
    ip = os.environ.get('ip')
    params = {'drPlanIds': dr_plan_id}
    response = requests.request("GET", "{base_url}/actions".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), params=params)
    if response.status_code == 200:
        return response.json()['actions']
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
    #                    dest_vc='system-test-vc01.qa01.eng.cohesity.com',dr_type='dhcp')
    # dr_plans  = get_dr_plans(name='profile_2')
    # for plan in dr_plans:
    #     activate(dr_plan_info=plan)
    # # delete all profile_2 plans
    # plans = get_dr_plans(name='profile_2')[:12]
    # plans += get_dr_plans(name='profile_3')[:11]
    # for plan in plans:
    #     delete_dr_plan(dr_id=plan.get('id'))
    #
    # plans = get_dr_plans(name='profile_2')
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



    dr_plans = get_dr_plans('profile_2')
    random_plans=random.sample(dr_plans,10)
    # dr_plans = get_dr_plans('profile_3')
    # random_plans += random.sample(dr_plans, 10)
    # dr_plans = get_dr_plans('profile_cdp')
    # random_plans += random.sample(dr_plans, 3)
    result = {}
    plan_names = []
    for plan in random_plans:
        print(plan.get('name'))
        if plan.get('name').startswith('profile_2'):
            result['profile_2'] = result.get('profile_2', 0) + 1
        elif plan.get('name').startswith('profile_3'):
            result['profile_3'] = result.get('profile_3', 0) + 1
        elif plan.get('name').startswith('profile_cdp'):
            result['profile_cdp'] = result.get('profile_cdp', 0) + 1
        plan_names.append(plan.get('name'))
        test_failover(dr_plan_info=plan)
    #
    print(result)
    print(plan_names)


    # plans = ['profile_2_app_247-dr_plan', 'profile_2_app_31-dr_plan', 'profile_2_app_61-dr_plan', 'profile_2_app_109-dr_plan', 'profile_2_app_127-dr_plan', 'profile_2_app_49-dr_plan', 'profile_2_app_73-dr_plan', 'profile_2_app_265-dr_plan', 'profile_2_app_157-dr_plan', 'profile_2_app_217-dr_plan', 'profile_3_app_28-dr_plan', 'profile_3_app_58-dr_plan', 'profile_3_app_16-dr_plan', 'profile_3_app_13-dr_plan', 'profile_3_app_1-dr_plan', 'profile_3_app_43-dr_plan', 'profile_3_app_22-dr_plan', 'profile_3_app_7-dr_plan', 'profile_3_app_46-dr_plan', 'profile_3_app_70-dr_plan', 'profile_cdp_2_app_4-dr_plan', 'profile_cdp_1_app_4-dr_plan', 'profile_cdp_1_app_3-dr_plan']
    # print(len(plans))
    # for plan in plans:
    #     plan = get_dr_plans(name=plan)[0]
    #     teardown(dr_plan_id=plan.get('id'))
