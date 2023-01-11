import os
import concurrent.futures
import requests

from cluster.connection import setup_cluster_automation_variables_in_environment
from cluster.virtual_machines import get_protected_vm_info, get_vm_protection_info, generate_vm_names
from site_continuity.connection import get_base_url, get_headers, set_environ_variables
from site_continuity.sites import get_sites

def get_applications(name=None):
    ip = os.environ.get('ip')
    params = None
    if name is not None:
        params = {'names':name}
    response = requests.request("GET", "{base_url}/applications".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), params=params)
    if response.status_code == 200:
        response = response.json()
        apps = response.get('applications')
        return apps
    else:
        print("Unsuccessful to get applications info - {}".format(response.status_code))
        return None


def create_application(app_name, vm_list, site_name="st-site-con-tx"):
    def get_object_param_from_vm(vm):
        print("getting object param of vm - {}".format(vm))
        data = {"type": "virtualMachine"}
        vm_info = get_protected_vm_info(vm)
        if vm_info:
            data["id"] = vm_info.get("id")
        else:
            print("could not add vm - {vm_name}".format(vm_name=vm))
            return
        res = get_vm_protection_info(vm)
        policy_id = res.get("policyId")
        policy_id = policy_id.rsplit(":", maxsplit=1)[0]
        protection_id = "{policy_id}:{protection_id}".format(policy_id=policy_id, protection_id=res['id'])
        data["virtualMachineParams"] = {"protectionGroupId": protection_id}
        return data
    ip = os.environ.get('ip')
    site_info = get_sites(site_name)
    if not site_info:
        print("Unsuccessful to get site info")
        return
    objectParams = []
    future_to_vm = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(vm_list)) as executor:
        for vm in vm_list:
            arg = (vm, )
            future_to_vm[executor.submit(get_object_param_from_vm, *arg)] = vm
    for future in concurrent.futures.as_completed(future_to_vm):
        vm = future_to_vm[future]
        try:
            res = future.result()
            if res:
                objectParams.append(res)
        except Exception as exc:
            print("%r generated an exception: %s" % (vm, exc))
        else:
            print("got object param result of vm {}".format(vm))

    data = {
    "name": app_name,
    "siteId": site_info.get('id'),  # get from get_sites("st-site-con-tx")
    "spec": {
        "source": {
            "environment": "vmware",
            "vmwareParams": {
                "sourceType": "vCenter",
                "vCenterParams": {
                    "objectId": 1  # todo get dynamically
                }
            }
        },
        "components": [
            {
                "type": "objects",
                "objectParams": objectParams
            }
        ]
    }
    }
    response = requests.request("POST", "{base_url}/applications".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(),json=data)
    if response.status_code == 201:
        print("Successfully created application named - {application_name} with Vms - {vm_list}".format(
            application_name=app_name,
            vm_list = vm_list
        ))
        return response
    else:
        print("Unsuccessful to create application named - {application_name} with Vms - {vm_list}".format(
            application_name=app_name,
            vm_list=vm_list
        ))
        return response

def delete_application(app_name):
    app_info = get_applications(app_name)[0]
    response = requests.request("DELETE", "{base_url}/applications/{app_id}".format(base_url=get_base_url(ip),
                                                                                    app_id=app_info.get("id")),
                                verify=False,
                                headers=get_headers())
    if response.status_code == 204:
        print("App - {} is successfully deleted".format(app_name))
    else:
        print("Unable to delete app - {}".format(app_name))


if __name__ == '__main__':
    ip = 'helios-test1.cohesitycloud.co'
    set_environ_variables({'ip': ip})
    # res = get_applications(name="auto-test-apps")
    res = delete_application("auto-test-apps")
    print(res)