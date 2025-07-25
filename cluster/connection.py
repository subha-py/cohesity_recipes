import os
import random
from itertools import cycle

import requests

from s3.utils.connector import get_s3_client, get_endpoint, get_s3_resource


def get_access_token(cluster_ip, username="admin", password="Syst7mt7st", domain="local"):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    data = {
        "password": password,
        "username": username
    }
    response = requests.request("POST", "https://{}/v2/access-tokens".format(cluster_ip),
                                verify=False, headers=headers, json=data)
    if response.status_code == 201:
        response_data = response.json()
        os.environ.setdefault("accessToken", response_data['accessToken'])
        return response_data['accessToken']
    else:
        print("could not get accesstoken")
        return None


def get_access_keys(cluster_ip, username="admin", password="Syst7mt7st", domain="local", access_token=None):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    if not access_token:
        access_token = os.environ.get("accessToken")
        if not access_token and not get_access_token(cluster_ip, username=username, password=password,
                                                     domain=domain):
            raise EnvironmentError("Please provide access token")
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET",
                                "https://{}/irisservices/api/v1/public/users?domain={}".format(cluster_ip, domain),
                                verify=False, headers=headers)
    if response.status_code == 200:
        response_data = response.json()[0]
        os.environ.setdefault("s3AccessKeyId", response_data['s3AccessKeyId'])
        os.environ.setdefault("s3SecretKey", response_data['s3SecretKey'])
        return response_data


def get_node_ips(cluster_ip, username="admin", password="Syst7mt7st", domain="local", access_token=None):
    # node_ip_string = '10.14.7.11,10.14.7.12'
    # os.environ.setdefault("node_ips", node_ip_string)
    # return node_ip_string
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    if not access_token:
        access_token = os.environ.get("accessToken")
        if not access_token and not get_access_token(cluster_ip, username=username, password=password,
                                                     domain=domain):
            raise EnvironmentError("Please provide access token")
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET", "https://{}/v2/clusters".format(cluster_ip), verify=False,
                                headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        nope_ip_string = response_data['nodeIps']
        node_ips = nope_ip_string.split(",")
        random.shuffle(node_ips)
        node_ip_string = ','.join(node_ips)
        os.environ.setdefault("node_ips", node_ip_string)
        return node_ips

    else:
        print("could not get node - ips")
        return None


def setup_cluster_automation_variables_in_environment(cluster_ip, username="admin", password="Syst7mt7st",
                                                      domain="local"):
    get_access_token(cluster_ip,username,password,domain)
    get_access_keys(cluster_ip, domain)
    get_node_ips(cluster_ip)


def get_client_cycle():
    ips = os.environ.get("node_ips").split(",")
    client_list = []
    for ip in ips:
        client = get_s3_client(get_endpoint(ip), os.environ.get("s3AccessKeyId"), os.environ.get("s3SecretKey"))
        if client:
            client_list.append(client)
    return cycle(client_list)


def get_resource_cycle():
    ips = os.environ.get("node_ips").split(",")
    client_list = []
    for ip in ips:
        client = get_s3_resource(get_endpoint(ip), os.environ.get("s3AccessKeyId"), os.environ.get("s3SecretKey"))
        if client:
            client_list.append(client)
    return cycle(client_list)


def get_base_url(version=1):
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    if version == 1:
        return "https://{ip}/irisservices/api/v1/public".format(ip=ip)


def get_headers():
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    if os.environ.get('accessToken'):
        headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    return headers


if __name__ == '__main__':
    setup_cluster_automation_variables_in_environment(cluster_ip="10.14.7.5", )
    ip = random.choice(os.environ.get("node_ips").split(","))
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET", "https://10.14.7.5/irisservices/api/v1/public/protectionSources/objects",
                                verify=False, headers=headers)
    if response.status_code == 200:
        response = response.json()
    print(response)