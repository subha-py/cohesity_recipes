import os
import requests
from site_continuity.connection import get_base_url, get_headers, set_environ_variables
from site_continuity.sites import get_sites
from cluster.connection import \
    (get_base_url as cohesity_base_url,
     get_headers as get_cohesity_headers,
     setup_cluster_automation_variables_in_environment
     )
from vmware.connection import find_by_moid
from cluster.virtual_machines import get_vc_id
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
                                                                  'clusterId': 27512,
                                                                  'clusterMoRef': 'domain-c4006',
                                                                  'dataCenterId': 18170,
                                                                  'dataCenterMoRef': 'datacenter-4001',
                                                                  'dataStoreId': 27511,
                                                                  'dataStoreMoRef': 'datastore-4012',
                                                                  'networkPortGroupId': 28450,
                                                                  'networkPortGroupMoRef': 'network-49794',
                                                                  'resourcePoolId': 27555,
                                                                  'resourcePoolMoRef': 'resgroup-4007'},
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
                    description=None, rpo=None, dr_type='dhcp',):
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
    ip = os.environ.get('ip')
    response = requests.request("POST", "{base_url}/dr-plans".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), json=data)
    status_code = response.status_code
    if response.status_code == 201:
        print("Successfully created dr_plan named - {dr_name}".format(
            dr_name=name
        ))
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
    ip = os.environ.get('ip')
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
