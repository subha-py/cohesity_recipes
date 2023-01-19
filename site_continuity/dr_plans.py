import os

import requests
from site_continuity.connection import get_base_url, get_headers, set_environ_variables
from site_continuity.sites import get_sites
from site_continuity.applications import get_applications
from cluster.connection import \
    (get_base_url as cohesity_base_url,
     get_headers as get_cohesity_headers,
     setup_cluster_automation_variables_in_environment
     )
from vmware.connection import find_by_moid
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

def get_dhcp_vmware_params(dr_plan_name):
    adict =  {'sourceType': 'vCenter',
                                    'vCenterParams': {
                                        'objectId': 17704, #todo get this info dynamically
                                        'resourceProfiles': [
                                            {
                                                'defaultResourceSet': {
                                                    'computeConfig': {
                                                              'clusterId': 17710,
                                                              'clusterMoRef': 'domain-c9202',
                                                              'dataCenterId': 16207,
                                                              'dataCenterMoRef': 'datacenter-8647',
                                                              'dataStoreId': 16215,
                                                              'dataStoreMoRef': 'datastore-8658',
                                                              'networkPortGroupId': 17481,
                                                              'networkPortGroupMoRef': 'network-8661',
                                                              'resourcePoolId': 17711,
                                                              'resourcePoolMoRef': 'resgroup-9203'},
                                                    'name': '{name}-default-resource-set'.format(name=dr_plan_name)},
                                                'ipConfig': {
                                                    'configurationType': 'DHCP',
                                                    'dhcpConfig': {
                                                        'dnsServers': [
                                                            '10.2.38.16'],
                                                        'dnsSuffixes': [
                                                            'qa01.eng.cohesity.com']}},
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
def get_static_vmware_params(app_info, source_vc):
    adict = {
            'sourceType': 'vCenter',
            'vCenterParams': {
                'objectId': 16205,
                'resourceProfiles': [{
                                     'customResourceSets': [],
                                     'defaultResourceSet': {
                                         'computeConfig': {
                                             'clusterId': 17710,
                                             'clusterMoRef': 'domain-c9202',
                                             'dataCenterId': 16207,
                                             'dataCenterMoRef': 'datacenter-8647',
                                             'dataStoreId': 16216,
                                             'dataStoreMoRef': 'datastore-8657',
                                             'networkPortGroupId': 17481,
                                             'networkPortGroupMoRef': 'network-8661',
                                             'resourcePoolId': 17711,
                                             'resourcePoolMoRef': 'resgroup-9203'},
                                         'name': 'Default resource set'},
                                     'ipConfig': {
                                         'configurationType': 'Static',
                                         'staticConfig': {
                                             'configOption': 'Manual',
                                             'manualIpConfig': {
                                                 'manualObjectLevelConfig': build_manual_object_level_config(app_info, source_vc=source_vc)},
                                             'networkMappingConfig': None}},
                                     'name': 'static_res'}]}}
    return adict
def create_dr_plan(name, app_name, primary_site="st-site-con-tx", secondary_site="st-site-con-rx", source_vc='10.14.22.105' , description=None, rpo=None,
                   dr_type='dhcp'):
    primary_site_info = get_sites(primary_site)[0]
    secondary_site_info = get_sites(secondary_site)[0]
    if not description:
        description = name
    if not rpo:
        rpo = {'frequency': 4, 'unit': 'Hours'}
    app_info = get_applications(app_name)[0]
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
                                        'objectId': 17704, #todo get this info dynamically
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
        data['drSite']['source']['vmwareParams'] = get_dhcp_vmware_params(name)
    elif dr_type == 'static':
        data['drSite']['source']['vmwareParams'] = get_static_vmware_params(app_info, source_vc=source_vc) # source vc is required to get the ips of vms
    response = requests.request("POST", "{base_url}/dr-plans".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), json=data)
    if response.status_code == 201:
        print("Successfully created dr_plan named - {dr_name}".format(
            dr_name=name
        ))
        return response
    else:
        print("Unsuccessful to create dr_plan named - {dr_name}".format(
            dr_name=name
        ))
        return response

def delete_dr_plan(dr_plan_name):
    dr_id = get_dr_plans(dr_plan_name)[0]
    response = requests.request("DELETE", "{base_url}/dr-plans/{dr_id}".format(base_url=get_base_url(ip),
                                dr_id=dr_id.get("id")),
                                verify=False,
                                headers=get_headers())
    if response.status_code == 204:
        print("dr_plan - {} is successfully deleted".format(dr_plan_name))
    else:
        print("Unable to delete app - {}".format(dr_plan_name))
        return response

def delete_all():
    dr_plans = get_dr_plans()
    for dr_plan in dr_plans:
        delete_dr_plan(dr_plan.get('name'))

def activate(name):
    dr_plan_info = get_dr_plans(name)[0]
    data = {'action': 'Activate'}
    response = requests.request("POST", "{base_url}/dr-plans/{plan_id}/actions".format(base_url=get_base_url(ip),
                                                                                       plan_id=dr_plan_info.get('id')),
                                verify=False,
                                headers=get_headers(), json=data)
    if response.status_code == 201:
        print('{name} is successfully activated'.format(name=name))
    else:
        response = response.json()
        print('unsuccessful to activate {name}, due to error - {error}'.format(name=name,
                                                                               error=response.get('errorMessage')))

if __name__ == '__main__':
    ip = 'helios-sandbox.cohesity.com'
    set_environ_variables({'ip': ip})
    setup_cluster_automation_variables_in_environment('10.14.7.5')
    create_dr_plan('static-auto',app_name='static-auto', dr_type='static')