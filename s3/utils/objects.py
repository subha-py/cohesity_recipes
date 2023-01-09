from uuid import uuid4
import random
from cluster.connection import setup_cluster_automation_variables_in_environment, get_resource_cycle, get_client_cycle
from s3.utils.connector import get_endpoint, get_s3_client
import os
from multiprocessing import Pool, cpu_count
from multiprocessing.dummy import Pool as ThreadPool
import concurrent.futures
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
    if list_response.get('Contents'):
        for content in list_response['Contents']:
            keys.append(content['Key'])
        return keys
    else:
        print("Bucket is empty")
        return

def put_random_object_tags(bucket_name, keys):
    def put_tag(bucket_name, key, tag_key, tag_val):
        print("working on bucket - {}, key - {}".format(bucket_name, key))
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
            return put_tags_response
        except Exception as ex:
            print(ex)
    tags = {
        'k1': 'v1',
        'k2': 'v2',
        'k3': 'v3',
        'k4': 'v4'
    }
    if len(keys) < 1:
        print("Nothing to run for bucket - {}".format(bucket_name))
        return False
    future_to_key = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(keys)) as executor:
        for key in keys:
            print("working on bucket - {}, key - {}".format(bucket_name, key))
            tag_key = random.choice(list(tags.keys()))
            tag_val = tags[tag_key]
            arg = (bucket_name, key, tag_key, tag_val)
            future_to_key[executor.submit(put_tag, *arg)] = key
        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            try:
                res = future.result()
            except Exception as exc:
                print("%r generated an exception: %s" % (key,exc))
            else:
                print("Tag is placed - {}".format(key))

def get_object_info(bucket_name, key, resource=None):
    if not resource:
        resource_cycle = get_resource_cycle()
        resource = next(resource_cycle)
    return resource.Object(bucket_name, key)

if __name__ == '__main__':
    setup_cluster_automation_variables_in_environment(cluster_ip="10.2.195.33")
    res = get_object_info('LCMTestBucket_Random_6','(7WoPzIU/1.Zlnl3qxwS9cHVcAq.rnd')
    print(res)