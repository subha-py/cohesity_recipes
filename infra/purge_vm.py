from infra.vcentre.setup.connector import get_vms_from_dc
from infra.vcentre.virtual_machines.power_ops.off import PowerOffVm
from infra.vcentre.virtual_machines.contants import ORACLE_DC_VMS

if __name__ == '__main__':
    for vm in get_vms_from_dc():
        if vm.name not in ORACLE_DC_VMS:
            try:
                print(f'Powering off... {vm.name}')
                PowerOffVm(vm)
            except Exception as ex:
                print(f'Got exception - {ex} while powering off {vm.name}')