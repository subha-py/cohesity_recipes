import os

import requests

from cluster.connection import setup_cluster_automation_variables_in_environment
from cluster.protection import get_protection_info
from cluster.virtual_machines import get_protected_vm_info, get_vm_protection_info, get_vc_id
from site_continuity.connection import get_base_url, get_headers, set_environ_variables
from site_continuity.sites import get_sites


def get_applications(name=None):
    ip = os.environ.get('ip')
    params = None
    if name is not None:
        params = {'appNames': name}
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
                       protection_info=None, script=False, delay=False, split_script_vms=False):
    def get_object_param_from_vm(vm=None, vc_id=None, vm_id=None, protection_info=None, script=False):
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
                "scriptExecutionPath": "/home",
                "scriptParams": {
                    "name": "test.sh",
                    "content": 'IyEvYmluL3NoCgpmb3IgaSBpbiAkKHNlcSAxIDEwKQpkbwpkYXRlCnNsZWVwIDEwCmRvbmUK',
                    "arguments": ['\u003e/tmp/log.txt']
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
    if vm_id_list:
        vm_list = vm_id_list
    for index, vm in enumerate(vm_list):
        script_param = script
        if script and index >= (len(vm_list) // 2):
            script_param = False  # half of the vms should not have script
        kwargs = {
            'vc_id': vc_id,
            'protection_info': protection_info,
            'script': script_param
        }
        if vm_id_list:
            kwargs['vm_id'] = vm
        else:
            kwargs['vm'] = vm
        objectParams.append(get_object_param_from_vm(**kwargs))
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
            "components": []
        }
    }
    if script and split_script_vms:
        data['spec']['components'] = [
            {
                "type": "objects",
                "objectParams": objectParams[:len(objectParams) // 2]
            }
        ]
        if delay:
            delay_component = {
                "type": "delay",
                "delayParams": {
                    "unit": "Minutes",
                    "delay": 2
                }
            }
            data['spec']['components'].append(delay_component)
        data['spec']['components'].append(
            {
                "type": "objects",
                "objectParams": objectParams[len(objectParams) // 2:]
            }
        )
    else:
        data['spec']['components'] = [
            {
                "type": "objects",
                "objectParams": objectParams
            }
        ]
    response = requests.request("POST", "{base_url}/applications".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), json=data)
    if response.status_code == 201:
        print("Successfully created application named - {application_name} with Vms - {vm_list}".format(
            application_name=app_name,
            vm_list=vm_list
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


def delete_application(app_name=None, app_id=None):
    if app_id is None:
        app_info = get_applications(app_name)[0]
        app_id = app_info.get("id")
    response = requests.request("DELETE", "{base_url}/applications/{app_id}".format(base_url=get_base_url(ip),
                                                                                    app_id=app_id),
                                verify=False,
                                headers=get_headers())
    if response.status_code == 204:
        print("App - {} is successfully deleted".format(app_id))
    else:
        response = response.json()
        print("Unsuccessful to delete application - {app_id} due to error - {error}".format(
            app_id=app_id,
            error=response.get('errorMessage')
        ))


def delete_all():
    apps = get_applications()
    for app in apps:
        delete_application(app.get('name'))


def get_replicated_snapshots(app_id):
    ip = os.environ.get('ip')
    response = requests.request("GET",
                                "{base_url}/applications/{app_id}/replicatedSnapshots".format(base_url=get_base_url(ip),
                                                                                              app_id=app_id),
                                verify=False,
                                headers=get_headers())
    if response.status_code == 200:
        object_snapshots = response.json()['objectSnapshots']  # todo sort me
        object_snapshots.sort(key=lambda x: x['replicatedSnapshots'][0]['snapshotTimeUsecs'], reverse=True)
        return object_snapshots
    else:
        print("Unsuccessful to get applications info - {}".format(response.status_code))
        return None


if __name__ == '__main__':
    ip = 'helios-sandbox.cohesity.com'
    set_environ_variables({'ip': ip})
    setup_cluster_automation_variables_in_environment('10.14.7.5')
    # # profile 3 apps
    protection_info = get_protection_info('profile_2_pg')
    source_ids = protection_info['sourceIds']
    number_of_vms_per_app = 6
    for i in range(0, len(source_ids), number_of_vms_per_app):
        create_application(app_name='phase_2_profile_2_app_{}'.format(i + 1),
                           vm_id_list=source_ids[i:i + number_of_vms_per_app],
                           source_vc='10.14.22.105',
                           protection_info=protection_info,
                           script=True, delay=True, split_script_vms=True)
