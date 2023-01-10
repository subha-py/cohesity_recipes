import os

import requests
from site_continuity.connection import get_base_url, get_headers, set_environ_variables

def get_sites(name=None):
    ip = os.environ.get('ip')
    response = requests.request("GET", "{base_url}/sites".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers())
    if response.status_code == 200:
        response = response.json()
        sites = response.get('sites')
        if name is not None and sites:
            for site in sites:
                if site.get('name') == name:
                    return site
        return response
    else:
        print("Unsuccessful to get applications info - {}".format(response.status_code))
        return None

if __name__ == '__main__':
    ip = 'helios-test1.cohesitycloud.co'
    set_environ_variables({'ip': ip})
    res = get_sites('st-site-con-tx')
    print(res)