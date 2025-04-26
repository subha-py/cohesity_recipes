import ipaddress
import concurrent.futures
import subprocess
from pprint import PrettyPrinter

def ip_assigned(ip):
    cmd = 'ping -c 5 {ip}'.format(ip=ip)
    cmd_args = cmd.split()
    exit_code = subprocess.run(cmd_args, stdout=subprocess.DEVNULL)
    if exit_code.returncode == 0:
        return True
    return False


# LOWER_LIMIT = '10.14.69.121'
# UPPER_LIMIT = '10.14.70.168'
LOWER_LIMIT = '10.3.56.2'
UPPER_LIMIT = '10.3.63.255'

def ping_ips_in_parallel():
    lower_ip_obj = ipaddress.IPv4Address(LOWER_LIMIT)
    upper_ip_obj = ipaddress.IPv4Address(UPPER_LIMIT)
    free_ips = []
    while lower_ip_obj < upper_ip_obj:
        free_ips.append(lower_ip_obj)
        lower_ip_obj += 1
    future_to_ip = {}
    with (concurrent.futures.ThreadPoolExecutor(max_workers=min(128,
                                            len(free_ips))) as executor):
        for ip_obj in free_ips:
            ip_str = str(ip_obj)
            arg = (str(ip_str),)
            future_to_ip[executor.submit(ip_assigned, *arg)] = ip_str
    result = []
    for future in concurrent.futures.as_completed(future_to_ip):
        ip = future_to_ip[future]
        try:
            res = future.result()
            if not res:
                result.append(ipaddress.IPv4Address(ip))
        except Exception as exc:
            print("%r generated an exception: %s" % (ip, exc))
    result = sorted(result)
    return [str(x) for x in result]


if __name__ == '__main__':
    result = ping_ips_in_parallel()
    pp = PrettyPrinter(indent=4)
    pp.pprint(result)
