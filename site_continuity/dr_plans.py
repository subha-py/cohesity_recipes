import os

import requests
from site_continuity.connection import get_base_url, get_headers, set_environ_variables

def get_dr_plans():
    ip = os.environ.get('ip')
    response = requests.request("GET", "{base_url}/dr-plans".format(base_url=get_base_url(ip)), verify=False, headers=get_headers())
    if response.status_code == 200:
        response = response.json()
        return response
    else:
        print("Unsuccessful to get dr plan info - {}".format(response.status_code))
        return None

if __name__ == '__main__':
    ip = 'helios-test1.cohesitycloud.co'
    set_environ_variables({'ip': ip})
    res = get_dr_plans()
    print(res)