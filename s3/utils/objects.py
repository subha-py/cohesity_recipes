from uuid import uuid4
import random
from cluster.connection import setup_cluster_automation_variables_in_environment, get_client_cycle
from s3.utils.connector import get_endpoint, get_s3_client
import os
from multiprocessing import Pool, cpu_count
def get_random_object_keys(client, bucket_name, count=1):
    keys = []
    try:
        list_response = client.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=count,
            StartAfter=str(uuid4()),
        )
    except Exception as ex:
        print("could not get keys for bucket name - {}, due to - {}".format(bucket_name, ex))
        return keys
    for content in list_response['Contents']:
        keys.append(content['Key'])
    return keys

def put_random_object_tags(bucket_name, keys):
    def put_tag(bucket_name, key, tag_key, tag_val):
        ips = os.environ.get("node_ips").split(",")
        ip = random.choice(ips)
        client = get_s3_client(get_endpoint(ip), os.environ.get("s3AccessKeyId"), os.environ.get("s3SecretKey"))
        try:
            put_tags_response = client.put_object_tagging(
                Bucket=bucket_name,
                Key=key,
                Tagging={
                    'TagSet': [
                        {
                            'Key': tag_key,
                            'Value': tag_val
                        },
                    ]
                }
            )
        except Exception as ex:
            print(ex)
    tags = {
        'k1': 'v1',
        'k2': 'v2',
        'k3': 'v3',
        'k4': 'v4'
    }
    pool = Pool(processes=cpu_count() - 1)
    for key in keys:
        print("working on bucket - {}, key - {}".format(bucket_name, key))
        tag_key = random.choice(list(tags.keys()))
        tag_val = tags[tag_key]
        arg = (bucket_name, key, tag_key, tag_val)
        pool.apply_async(put_tag, args=arg)
    pool.close()
    pool.join()
