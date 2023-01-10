import requests
import os

def get_headers(api_key="dd5e7e8d-9245-4d61-6ab6-c23913c85dd8"):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    if not api_key and os.environ.get("sitecon_apikey"):
        headers['apiKey'] = os.environ.get("sitecon_apikey") #todo: get api key from url, this is not working
    elif api_key is not None:
        headers['apiKey'] = api_key
    return headers

def get_base_url(ip):
    return "https://{ip}/v2/mcm/site-continuity/2.0".format(ip=ip)
