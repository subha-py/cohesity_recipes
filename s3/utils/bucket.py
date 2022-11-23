from collections import namedtuple
from s3.utils.aws_uploader import AwsS3Uploader
def get_buckets_from_prefix(client, prefix, count=0):
    """
    
    :param client: 
    :param prefix: 
    :return: 
    """
    result = []
    try:
        response = client.list_buckets()
    except Exception as ex:
        print("Could not get buckets due to - {}".format(ex))
        return result
    for bucket in response['Buckets']:
        name = bucket['Name']
        if name.startswith(prefix):
            result.append(bucket['Name'])
        if 0 < count < len(result):
            break
    return result

# def put_file_to_bucket_with_mpu(origpath, destpath,access_key,secret_access_key,endpoint, awsprofile=None):
#     Args = namedtuple('Args', ['origpath', 'destpath', 'awsprofile','access_key','secret_access_key','endpoint'])
#     args = Args(origpath, destpath, awsprofile, access_key, secret_access_key, endpoint)
#     uploader = AwsS3Uploader(access_key=args.access_key,secret_access_key=args.secret_access_key, endpoint=args.endpoint)
#     if uploader.upload_file(args.origpath, args.destpath):
#         print('Success')
#     else:
#         raise ('Upload Complete but Invalid')

