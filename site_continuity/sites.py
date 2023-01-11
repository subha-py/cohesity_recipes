import os

import requests
from site_continuity.connection import get_base_url, get_headers, set_environ_variables

def get_sites(name=None):
    ip = os.environ.get('ip')
    params = None
    if name is not None:
        params = {'names': name}
    response = requests.request("GET", "{base_url}/sites".format(base_url=get_base_url(ip)), verify=False,
                                headers=get_headers(), params=params)
    if response.status_code == 200:
        response = response.json()
        sites = response.get('sites')
        return sites
    else:
        print("Unsuccessful to get applications info - {}".format(response.status_code))
        return None

if __name__ == '__main__':
    ip = 'helios-sandbox.cohesity.com'
    set_environ_variables({'ip': ip})
    res = get_sites('st-site-con-tx')
    print(res)