import os


def get_headers(api_key="a0ed756d-e490-46fe-7fae-bbfd3dba48fd"):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    if not api_key and os.environ.get("sitecon_apikey"):
        headers['apiKey'] = os.environ.get("sitecon_apikey")  # todo: get api key from url, this is not working
        # irisservices/api/v1/public/mcm/users/<username>/apiKeys
    elif api_key is not None:
        headers['apiKey'] = api_key
    return headers


def get_helios_url(ip):
    return "https://{ip}/v2/mcm".format(ip=ip)


def get_base_url(ip):
    return "{helios_url}/site-continuity/2.0".format(helios_url=get_helios_url(ip))


def set_environ_variables(dictionary):
    for key, value in dictionary.items():
        os.environ.setdefault(key, value)
    return
