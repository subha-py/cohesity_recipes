import atexit
import ssl
import sys

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import pbm, vim, VmomiSupport, SoapStubAdapter

if sys.version[0] < '3':
    input = raw_input


def getClusterInstance(clusterName, serviceInstance):
    content = serviceInstance.RetrieveContent()
    searchIndex = content.searchIndex
    datacenters = content.rootFolder.childEntity
    for datacenter in datacenters:
        cluster = searchIndex.FindChild(datacenter.hostFolder, clusterName)
        if cluster is not None:
            return cluster
    return None


def get_service_instance(system_info):
    if not system_info:
        raise SystemError('Please enter valid defaults')
    # For python 2.7.9 and later, the default SSL context has more strict
    # connection handshaking rule. We may need turn off the hostname checking
    # and client side cert verification.
    context = None
    if sys.version_info[:3] > (2, 7, 8):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    si = SmartConnect(host=system_info.get('host'),
                      user=system_info.get('user', 'administrator@vsphere.local'),
                      pwd=system_info.get('password', 'Cohe$1ty'),
                      port=int(system_info.get('port', '443')),
                      sslContext=context)

    atexit.register(Disconnect, si)
    return si


def get_cluster_cr(system_info=None, si=None):
    """

    :param system_info: system info
     system_info = {
        'host': '10.154.174.232',
        'username': 'administrator@vsphere.local',
        'password': 'Admin!23'
    }
    :return:
    """
    if not si:
        si = get_service_instance(system_info)
    # Detecting whether the host is vCenter or ESXi.
    aboutInfo = si.content.about

    if aboutInfo.apiType == 'VirtualCenter':
        majorApiVersion = aboutInfo.apiVersion.split('.')[0]
        if int(majorApiVersion) < 6:
            print('The Virtual Center with version %s (lower than 6.0) is not supported.'
                  % aboutInfo.apiVersion)
            return -1
        # getting clusterCr
        if system_info:
            clusterName = system_info.get('clusterName', 'StressVpx-1')
        else:
            clusterName = 'StressVpx-1'
        cluster = getClusterInstance(clusterName, si)
        return cluster
    else:
        raise NotImplemented('Other platforms are not supported as of now')


def get_vms_from_dc(system_info, names=None):
    si = get_service_instance(system_info)
    content = si.RetrieveContent()
    children = content.rootFolder.childEntity
    result = []
    for child in children:  # Iterate though DataCenters
        dc = child
        clusters = dc.hostFolder.childEntity
        for cluster in clusters:  # Iterate through the clusters in the DC
            # Add Clusters to data dict
            try:
                hosts = cluster.host  # Variable to make pep8 compliance
            except AttributeError as ex:
                continue
            for host in hosts:  # Iterate through Hosts in the Cluster
                hostname = host.summary.config.name
                # Add VMs to data dict by config name
                vms = host.vm
                for vm in vms:  # Iterate through each VM on the host
                    vm_name = vm.summary.config.name
                    if 'vCLS' not in vm_name and 'template' not in vm_name:
                        if names:
                            if vm_name in names:
                                result.append(vm)
                            if len(names) == len(result):
                                return result
                        else:
                            result.append(vm)

    return result


def get_vms_from_dc_with_prefix(system_info, prefix=None, datacentre=None):
    si = get_service_instance(system_info)
    content = si.RetrieveContent()
    children = content.rootFolder.childEntity
    result = []
    for child in children:
        if datacentre is not None and datacentre != child.name:
            continue
            # Iterate though DataCenters
        dc = child
        clusters = dc.hostFolder.childEntity
        for cluster in clusters:  # Iterate through the clusters in the DC
            # Add Clusters to data dict
            if "folder" in str(cluster).lower():
                continue
            hosts = cluster.host  # Variable to make pep8 compliance
            for host in hosts:  # Iterate through Hosts in the Cluster
                hostname = host.summary.config.name
                # Add VMs to data dict by config name
                vms = host.vm
                for vm in vms:  # Iterate through each VM on the host
                    vm_name = vm.summary.config.name
                    if 'vCLS' not in vm_name and 'template' not in vm_name:
                        if prefix:
                            if prefix in vm_name:
                                result.append(vm)
                        else:
                            result.append(vm)

    return result


def get_witness_host_from_dc(system_info):
    si = get_service_instance(system_info)
    content = si.RetrieveContent()
    children = content.rootFolder.childEntity
    for child in children:  # Iterate though DataCenters
        dc = child
        clusters = dc.hostFolder.childEntity
        for cluster in clusters:
            if isinstance(cluster, vim.ComputeResource):
                for host in cluster.host:
                    return host


def getAddedHosts(system_info, names=None):
    si = get_service_instance(system_info)
    content = si.RetrieveContent()
    children = content.rootFolder.childEntity
    for child in children:  # Iterate though DataCenters
        datacenter = child
        break
    hosts = []
    hostfolder = datacenter.hostFolder.childEntity
    for obj in hostfolder:
        if names is not None:
            if obj.host[0].name not in names:
                continue
        hosts.append(obj.host[0])
    return hosts


def get_host_from_dc(system_info, names=None):
    hosts = get_cluster_cr(system_info).host
    result = []
    for host in hosts:
        if names:
            if host.name in names:
                result.append(host)
        else:
            result.append(host)
    return result


def pbm_connect(stub_adapter, disable_ssl_verification=False):
    """Connect to the VMware Storage Policy Server

    :param stub_adapter: The ServiceInstance stub adapter
    :type stub_adapter: SoapStubAdapter
    :param disable_ssl_verification: A flag used to skip ssl certificate
        verification (default is False)
    :type disable_ssl_verification: bool
    :returns: A VMware Storage Policy Service content object
    :rtype: ServiceContent
    """

    if disable_ssl_verification:
        import ssl
        if hasattr(ssl, '_create_unverified_context'):
            ssl_context = ssl._create_unverified_context()
        else:
            ssl_context = None
    else:
        ssl_context = None

    VmomiSupport.GetRequestContext()["vcSessionCookie"] = \
        stub_adapter.cookie.split('"')[1]
    hostname = stub_adapter.host.split(":")[0]
    pbm_stub = SoapStubAdapter(
        host=hostname,
        version="pbm.version.version1",
        path="/pbm/sdk",
        poolSize=0,
        sslContext=ssl_context)
    pbm_si = pbm.ServiceInstance("ServiceInstance", pbm_stub)
    pbm_content = pbm_si.RetrieveContent()
    return pbm_content


def get_pbm_manager(system_info):
    si = get_service_instance(system_info)
    pbm_content = pbm_connect(si._stub, True)
    pm = pbm_content.profileManager
    return pm


def find_by_moid(system_info, moid):
    si = get_service_instance(system_info)
    vm = vim.VirtualMachine(moid)
    vm._stub = si._stub
    return vm


if __name__ == '__main__':
    system_info = {'host': 'system-test-vc02.qa01.eng.cohesity.com'}
    vm = find_by_moid(system_info, 'vm-18537')
    print(vm.guest.ipAddress)
