from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
import urllib3
import os
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from cluster.connection import setup_cluster_automation_variables_in_environment, get_base_url, get_headers
from cluster.protection import get_protection_info

from site_continuity.connection import get_helios_url, set_environ_variables, get_headers as site_con_headers
def get_protected_vm_info(vm_name, vc_id):
    response = requests.request("GET", "{base_url}/protectionSources/virtualMachines?vCenterId={vcentre_id}&names={vm_name}&protected=true".
                                format(base_url=get_base_url(), vm_name=vm_name, vcentre_id=vc_id),
                                verify=False, headers=get_headers())

    response = response.json()
    if response:
        return response[0]

def get_vm_protection_info(vm_name=None, vc_id=None, vm_id=None):
    if vm_id is None:
        vm_info = get_protected_vm_info(vm_name, vc_id)
        if vm_info:
            vm_id = vm_info.get("id")
        else:
            print("could not add vm - {vm_name}".format(vm_name=vm_name))
            return
    response = requests.request("GET",
                                "{base_url}/protectionObjects/summary?protectionSourceId={vm_id}".
                                format(base_url=get_base_url(), vm_id=vm_id),
                                verify=False, headers=get_headers())
    if response.status_code == 200:
        response = response.json()
        jobName = response['protectionJobs'][0]['jobName']
        protection_info = get_protection_info(jobName)
        return protection_info

def generate_vm_names(prefix, count, start_index):
    names = []
    for val in range(start_index, start_index+count):
        names.append("{prefix}{val}".format(prefix=prefix,val=val))
    return names

def get_vc_id(name):
    # https://helios-sandbox.cohesity.com/v2/mcm/data-protect/sources?excludeProtectionStats=true&environments=kVMware
    ip = os.environ.get('ip')
    response = requests.request("GET",
                                "{helios_url}/data-protect/sources?vsources?excludeProtectionStats=true&environments=kVMware".
                                format(helios_url=get_helios_url(ip)),
                                verify=False, headers=site_con_headers())

    response = response.json()
    if response:
        for source in response.get('sources'):
            if name in source.get('name'):
                return source['sourceInfoList'][0]['sourceId']

def get_vm_source_ids_from_pg(pg_name):
    return get_protection_info(pg_name)['sourceIds']

if __name__ == '__main__':
    ip = 'helios-sandbox.cohesity.com'
    set_environ_variables({'ip': ip})
    setup_cluster_automation_variables_in_environment(cluster_ip="10.14.7.5")
    res = get_protected_vm_info('sitecon-lin-001', vc_id='16201')
    print(res)
