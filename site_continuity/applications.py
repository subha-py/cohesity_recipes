import os
import concurrent.futures
import requests

from cluster.connection import setup_cluster_automation_variables_in_environment
from cluster.protection import get_protection_info
from cluster.virtual_machines import get_protected_vm_info, get_vm_protection_info, get_vm_source_ids_from_pg, get_vc_id
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


def create_application(app_name, source_vc, site_name="st-site-con-tx", vm_id_list=None, vm_list=None,
                       protection_info=None, script=False, delay=False):
    def get_object_param_from_vm(vm=None, vc_id=None, vm_id=None, protection_info=None, script=False): #todo if vm_id is provided vm name is not required
        if vm_id:
            vm = vm_id
        print("getting object param of vm - {}".format(vm))
        data = {"type": "virtualMachine"}

        if vm_id is not None:
            data["id"] = vm_id
        else:
            vm_info = get_protected_vm_info(vm, vc_id)
            if vm_info:
                data["id"] = vm_info.get("id")
            else:
                print("could not add vm - {vm_name}".format(vm_name=vm))
                return
        if protection_info is not None:
            protection_info = get_vm_protection_info(vm_id=data["id"], vc_id=vc_id)
        policy_id = protection_info.get("policyId")
        policy_id = policy_id.rsplit(":", maxsplit=1)[0]
        protection_id = "{policy_id}:{protection_id}".format(policy_id=policy_id, protection_id=protection_info['id'])
        data["virtualMachineParams"] = {"protectionGroupId": protection_id}
        if script:
            script_params = {
                "username": "root",
                "password": "root1234",
                "scriptExecutionPath": "/tmp",
                "scriptParams": {
                    "name": "scratch_2.py",
                    "content": os.environ.get('script_content'), #todo convert file to content dynamically
                    "arguments": ['2']
                }
            }
            data["virtualMachineParams"].update(script_params)
        return data
    ip = os.environ.get('ip')
    site_info = get_sites(site_name)[0]
    vc_id = get_vc_id(source_vc)
    if not site_info:
        print("Unsuccessful to get site info")
        return
    objectParams = []
    future_to_vm = {}
    if vm_id_list:
        vm_list = vm_id_list
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(vm_list)) as executor:
        for vm in vm_list:
            kwargs = {
                'vc_id': vc_id,
                'protection_info':protection_info,
                'script':script
            }
            if vm_id_list:
                kwargs['vm_id'] = vm
            else:
                kwargs['vm'] = vm
            future_to_vm[executor.submit(get_object_param_from_vm, **kwargs)] = vm
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
                    "objectId": vc_id
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
    if delay:
        delay_component = {
                "type": "delay",
                "delayParams": {
                    "unit": "Minutes",
                    "delay": 5
                }
            }
        data['spec']['components'].append(delay_component)
    response = requests.request("POST", "{base_url}/applications".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(),json=data)
    if response.status_code == 201:
        print("Successfully created application named - {application_name} with Vms - {vm_list}".format(
            application_name=app_name,
            vm_list = vm_list
        ))
        return response
    else:
        response = response.json()
        print("Unsuccessful to create application named - {application_name} with Vms - {vm_list} - \
              due to error - {error}".format(
            application_name=app_name,
            vm_list=vm_list,
            error=response.get('errorMessage')
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

def delete_all():
    apps = get_applications()
    for app in apps:
        delete_application(app.get('name'))

def get_replicated_snapshots(app_id):
    ip = os.environ.get('ip')
    response = requests.request("GET", "{base_url}/applications/{app_id}/replicatedSnapshots".format(base_url=get_base_url(ip),
                                                                                                     app_id=app_id), verify=False,
                                headers=get_headers())
    if response.status_code == 200:
        return response.json()['objectSnapshots']

    else:
        print("Unsuccessful to get applications info - {}".format(response.status_code))
        return None


if __name__ == '__main__':
    ip = 'helios-sandbox.cohesity.com'
    set_environ_variables({'ip': ip})
    setup_cluster_automation_variables_in_environment('10.14.7.5')
    # protection_info = get_protection_info('profile_2_pg')
    # source_ids = protection_info['sourceIds']
    # number_of_vms_per_app = 3
    # for i in range(0, len(source_ids), number_of_vms_per_app):
    #     create_application(app_name='profile_3_app_{}'.format(i+1),
    #                        vm_id_list=source_ids[i:i+number_of_vms_per_app],
    #                        source_vc='10.14.22.105',
    #                        protection_info=protection_info)
    # app = get_applications(name='subha-test-failover-app')[0]
    # print(app)
    source_ids = [24527, 24529, 24693]
    protection_info = get_protection_info('profile_2_pg')
    create_application(app_name='subha-auto-script',
                       vm_id_list=source_ids,
                       source_vc='system-test-vc02.qa01.eng.cohesity.com',
                       protection_info=protection_info, script=True, delay=True)