from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
import urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from cluster.connection import setup_cluster_automation_variables_in_environment, get_base_url, get_headers
from cluster.protection import get_protection_info

def get_protected_vm_info(vm_name, vcentre_id=17704): #todo take vcentre dynamically
    response = requests.request("GET", "{base_url}/protectionSources/virtualMachines?vCenterId={vcentre_id}&names={vm_name}&protected=true".
                                format(base_url=get_base_url(), vm_name=vm_name, vcentre_id=vcentre_id),
                                verify=False, headers=get_headers())

    response = response.json()
    if response:
        return response[0]

def get_vm_protection_info(vm_name):
    vm_info = get_protected_vm_info(vm_name)
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

if __name__ == '__main__':
    vm_names = generate_vm_names(prefix="VMST10", count=3, start_index=66)
    print(vm_names)
