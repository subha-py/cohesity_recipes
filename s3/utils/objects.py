from uuid import uuid4
import random


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

def put_random_object_tags(client_list_cycle, bucket_name, keys):
    tags = {
        'k1': 'v1',
        'k2': 'v2',
        'k3': 'v3',
        'k4': 'v4'
    }
    for key in keys:
        print("working on bucket - {}, key - {}".format(bucket_name, key))
        tag_key = random.choice(list(tags.keys()))
        tag_val = tags[tag_key]
        try:
            put_tags_response = next(client_list_cycle).put_object_tagging(
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
