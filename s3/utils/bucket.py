import random
from s3.utils.aws_highlvlapi.aws_transfer_manager import TransferCallback as aws_TransferCallback
import os
from boto3.s3.transfer import TransferConfig
from s3.utils.aws_uploader import Chunk
import string
from files.fio import get_fio_files
from multiprocessing import Pool, cpu_count
from s3.utils.objects import put_random_object_tags, get_random_object_keys
from cluster.connection import setup_cluster_automation_variables_in_environment, get_client_cycle
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

def get_fileSize(file_path):
    """
    Get the size of a file and return in Bytes
    :param file_path:
    :return: size of file in Bytes
    """
    # file size bytes
    return os.path.getsize(file_path)


def upload_simple_multi_part(client, local_file_path, bucket, remote_file_path):
    config = TransferConfig(multipart_chunksize=1024)
    callback = aws_TransferCallback(get_fileSize(local_file_path))
    client.upload_file(filename=local_file_path, bucket=bucket, key=remote_file_path, Config=config,
                       Callback=callback)


def start_multipart_upload(client,bucket, dest_path):
    multipart_meta = client.create_multipart_upload(
        Bucket=bucket,
        Key=dest_path,
    )

    return multipart_meta

def upload_multipart_part(client, upload_meta, chunk):
    with open(chunk.path, 'rb') as reader:
        response = client.upload_part(
            Bucket=upload_meta['Bucket'],
            Key=upload_meta['Key'],
            PartNumber=chunk.part_number,
            UploadId=upload_meta['UploadId'],
            Body=reader
        )
    etag = response['ETag'][1:-1]
    if chunk.validate(etag):
        return True
    else:
        # TODO Retry
        raise ('part {} not valid - {}'.format(chunk.part_number, chunk.path))


def abort_multipart_upload(client, upload_meta):
    client.abort_multipart_upload(
        Bucket=upload_meta['Bucket'],
        Key=upload_meta['Key'],
        uploadId=upload_meta['UploadId'],
    )


def complete_multipart_upload(client,upload_meta, chunks):
    # == Create multipart upload dict ==
    part_dict = {
        'Parts': []
    }

    for chunk in chunks:
        part_dict['Parts'].append(
            {
                'ETag': chunk.etag,
                'PartNumber': chunk.part_number
            }
        )

    # == Send Packet ==
    response = client.complete_multipart_upload(
        Bucket=upload_meta['Bucket'],
        Key=upload_meta['Key'],
        UploadId=upload_meta['UploadId'],
        MultipartUpload=part_dict
    )
    return response

def upload_custom_multi_part(client_list_cycle, bucket_name, local_file, remote_file_path, chunk_size_mib=4):
    # Init multi-part upload
    remote_file_name = remote_file_path.split('/')[-1]
    tmp_dir = "{}/{}".format(os.path.dirname(local_file), remote_file_name)
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)
    multipart_meta = start_multipart_upload(next(client_list_cycle), bucket_name, remote_file_path)
    try:
        file_size = get_fileSize(local_file)
        read_pos = 0
        chunk_part = 1
        chunk_size_bytes = chunk_size_mib * 1024 * 1024
        chunk_basename = os.path.splitext(os.path.basename(local_file))[0]
        chunks = []
        # with open(origin_path, 'rb') as reader:
        while read_pos < file_size:
            # Update chunk name (path)
            chunk_path = os.path.join(tmp_dir, '{}_chunk_{}'.format(chunk_basename, chunk_part))
            # Create a chunk
            chunk = Chunk(chunk_path, chunk_part, chunk_size_bytes=chunk_size_bytes)
            chunk.create_chunk(local_file)
            chunks.append(chunk)
            print('uploading[id: {}] chunk {}:{} to {} - with chunksize - {}mib'.format(multipart_meta['UploadId'], os.path.basename(local_file), chunk_part,
                                                                                bucket_name, chunk_size_mib,
                                                                                ))
            upload_multipart_part(next(client_list_cycle), multipart_meta, chunk)
            # update read position and chunk part
            read_pos += chunk_size_bytes
            chunk_part += 1
            chunk.destroy()
            # == Complete Upload ==
        os.rmdir(tmp_dir)
        multipart_complete_meta = complete_multipart_upload(next(client_list_cycle), multipart_meta, chunks)

    except Exception as ex:
        # Cleanup multipart upload if exception
        print("Something went wrong while uploading file - {} - {} - details - {}".format(bucket_name, ex, multipart_meta))
        abort_multipart_upload(next(client_list_cycle), multipart_meta)
        os.rmdir(tmp_dir)

def create_random_prefix():
    chars = list(string.ascii_letters)[:-6] # last six are tabs, binary sign
    depth = random.randint(1,256)
    prefix = []
    for _ in range(depth):
        prefix.append(random.choice(chars))
    return '/'.join(prefix)

def create_random_filename():
    chars = list(string.ascii_letters)[:-6] # last six are tabs, binary sign
    file_name_size = random.randint(1,256)
    result = []
    for _ in range(file_name_size):
        result.append(random.choice(chars))
    result.append('.sb')
    return ''.join(result)

def upload_files_in_bucket(bucket_name, local_directory="/home/cohesity/FioFiles", chunksizes=None):
    client_list_cycle = get_client_cycle()
    files_to_upload = get_fio_files(local_directory)
    random.shuffle(files_to_upload)
    if chunksizes is None:
        chunksizes = [2, 4, 8, 16, 24]
    for file in files_to_upload:
        prefix = create_random_prefix()
        remote_file_name = create_random_filename()
        remote_file_path = "{}/{}".format(prefix,remote_file_name)
        upload_custom_multi_part(client_list_cycle, bucket_name, file, remote_file_path, random.choice(chunksizes))
        print(f"file - {file} is suceesfully uploaded to - {bucket_name}:{remote_file_path}")
        put_random_object_tags(client_list_cycle, bucket_name, [remote_file_path])

def upload_files_in_buckets(buckets, local_directory="/home/cohesity/FioFiles", chunksizes=None):
    pool = Pool(processes=cpu_count() - 1)
    for bucket in buckets:
        arg = (bucket, local_directory, chunksizes)
        pool.apply_async(upload_files_in_bucket, args=arg)
    pool.close()
    pool.join()
    return

def overwrite_files_in_bucket(bucket_name, local_directory="/home/cohesity/FioFiles", chunksizes=None):
    client_list_cycle = get_client_cycle()
    files_to_upload = get_fio_files(local_directory)
    random.shuffle(files_to_upload)
    if chunksizes is None:
        chunksizes = [2, 4, 8, 16, 24]
    for file in files_to_upload:
        remote_file_path = get_random_object_keys(next(client_list_cycle), bucket_name)[0]
        upload_custom_multi_part(client_list_cycle, bucket_name, file, remote_file_path, random.choice(chunksizes))
        print(f"file - {file} is sucessfully overwritten to - {bucket_name}:{remote_file_path}")
        put_random_object_tags(client_list_cycle, bucket_name, [remote_file_path])