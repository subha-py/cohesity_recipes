import os

import requests
from site_continuity.connection import get_base_url, get_headers, set_environ_variables
from site_continuity.sites import get_sites
from site_continuity.applications import get_applications
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

def create_dr_plan(name, primary_site="st-site-con-tx", secondary_site="st-site-con-rx", app_name=None, description=None, rpo=None):
    primary_site_info = get_sites(primary_site)[0]
    secondary_site_info = get_sites(secondary_site)[0]
    if not description:
        description = name
    if not rpo:
        rpo = {'frequency': 4, 'unit': 'Hours'}
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
                    'vmwareParams': {'sourceType': 'vCenter',
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
                                                    'name': '{name}-default-resource-set'.format(name=name)},
                                                'ipConfig': {
                                                    'configurationType': 'DHCP',
                                                    'dhcpConfig': {
                                                        'dnsServers': [
                                                            '10.2.38.16'],
                                                        'dnsSuffixes': [
                                                            'qa01.eng.cohesity.com']}},
                                                'name': '{name}-profile'.format(name=name)
                                                }
                                            ]
                                        }
                                    }
                }
        },
        "rpo": rpo,

    }
    if app_name:
        data["appId"] = get_applications(app_name)[0].get('id')
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
    apps = get_applications()
    for app in apps:
        app_name = app.get('name')
        if 'profile_1' in app_name:
            create_dr_plan(name="{}-dr_plan".format(app_name),app_name=app_name)