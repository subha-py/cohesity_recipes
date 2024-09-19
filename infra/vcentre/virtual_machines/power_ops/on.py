from ipaddress import ip_address
import time
from project_utils.ssh import run_ssh_cmd
from infra.vcentre.utils.tasks import wait_for_vim_task

def WaitForIp(vmRef):
    """Wait for the assignment of vm ip"""
    waited = 0
    while waited < 5 * 60:
        ip = vmRef.guest.ipAddress
        print(f'waiting for ip[{vmRef.name}]: waited - {waited}, ip - {ip}')
        try:
            ip_obj = ip_address(ip)
            break
        except ValueError:
            waited += 5
            time.sleep(5)
    else:
        msg = "No IP after 5 minutes"
        print(msg)
    if ip_obj:
        cmd = 'rm -rf /etc/udev/rules.d/70-persistent-net.rules'
        for _ in range(3):
            try:
                run_ssh_cmd(ip, cmd)
                break
            except:
                time.sleep(60)
                continue

    return ip

def PowerOnVm(vmRef):
    if vmRef.runtime.connectionState != 'connected':
        raise Exception('PowerOnVm Exception: vm is not connected: connectionState=%s'
                        % vmRef.runtime.connectionState)
    elif vmRef.runtime.powerState == 'poweredOn':
        print('vm is already in power on state: %s' % vmRef.runtime.powerState)
        result = WaitForIp(vmRef)
    else:
        vimTask = vmRef.PowerOn()
        wait_for_vim_task(vimTask)
        result = WaitForIp(vmRef)
    return result
