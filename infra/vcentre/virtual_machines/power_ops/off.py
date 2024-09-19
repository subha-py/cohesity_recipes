from infra.vcentre.utils.tasks import wait_for_vim_task
from infra.vcentre.setup.connector import get_vms_from_dc
def PowerOffVm(vmRef):
    vimTask = vmRef.PowerOff()
    wait_for_vim_task(vimTask)
    return vimTask

if __name__=='__main__':
    vms = get_vms_from_dc()
    vmref = None
    for vm in vms:
        if vm.name == 'gk_rac_setup_mnmc_n5':
            vmref = vm
            break
    if vmref:
        PowerOffVm(vmref)

