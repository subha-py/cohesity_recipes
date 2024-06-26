import hashlib
import os
from enum import Enum
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

from s3.utils.aws_highlvlapi.aws_transfer_manager import TransferCallback as aws_TransferCallback


def get_awsUploader(args):
    if args.awsprofile is not None:
        return AwsS3Uploader(access_key=args.access_key, secret_access_key=args.secret_access_key,
                             aws_profile=args.awsprofile)
    # else:
    #     return AwsS3Uploader(multipart_limit_size_mib=512)


class AwsS3Uploader:
    def __init__(self, access_key, secret_access_key, endpoint, multipart_limit_size_mib=512,
                 aws_profile=None):  # old size = 4096
        self.file_parts = []

        # self.s3_resource = boto3.resource('s3')
        config = Config(retries={"max_attempts": 1, "mode": "standard"})
        self.s3_client = boto3.client = boto3.client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint,
            verify=False,
            config=config
        )
        self.multipart_limit_size = multipart_limit_size_mib * 1024 * 1024
        self.aws_profile = aws_profile

    def upload_file(self, file_path, dest_path):
        isSuccess = False

        # Check file size and convert to MiB
        file_size = get_fileSize(file_path)
        # Check if multipart
        if file_size > self.multipart_limit_size:
            isSuccess = self.__upload_multiPart_custom(file_path, dest_path, chunk_size_mib=100)
        else:
            isSuccess = self.__upload_singlePart(file_path, dest_path)
        # TODO Wait for threads to finish
        return isSuccess

    def validate_file(self, dest_path, md5):
        etag = self.__get_etag(dest_path)

        if md5 == etag:
            return True
        else:
            return False

    def __upload_singlePart(self, origin_path, dest_path, validate=True):
        object_key = os.path.basename(origin_path)

        # == Upload ==
        # callback = aws_TransferCallback(get_fileSize(origin_path))
        # config = TransferConfig(multipart_threshold=self.multipart_limit_size)

        prefix = self.__get_file_key(dest_path)
        if len(prefix) == 0:
            prefix = os.path.basename(origin_path)

        self.s3_client.upload_file(
            origin_path,
            self.__get_bucketname(dest_path),
            prefix,
            # Config=config,
            # Callback=callback
        )

        # == Validate ==
        md5 = get_md5(origin_path)
        etag = self.__get_etag(dest_path)
        if md5 != etag:
            raise ('Validation Error: MD5 mismatch')

        return True

    def __upload_multiPart_simple(self, origin_path, dest_path):
        object_key = os.path.basename(origin_path)

        callback = aws_TransferCallback(get_fileSize(origin_path))
        config = TransferConfig(multipart_chunksize=1024)

        self.s3_client.Bucket(dest_path).upload_file(
            origin_path,
            object_key,
            Config=config,
            Callback=callback
        )

        return callback.thread_info

    def __start_multipart_upload(self, origin_path, dest_path):
        object_key = os.path.basename(origin_path)

        multipart_meta = self.s3_client.create_multipart_upload(
            Bucket=self.__get_bucketname(dest_path),
            Key=self.__get_file_key(dest_path),
        )

        return multipart_meta

    def __upload_multipart_part(self, upload_meta, chunk, retries=3):
        with open(chunk.path, 'rb') as reader:
            response = self.s3_client.upload_part(
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
            raise ('part {} not valid'.format(chunk.part_number))

    def __complete_multipart_upload(self, upload_meta, chunks):
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
        response = self.s3_client.complete_multipart_upload(
            Bucket=upload_meta['Bucket'],
            Key=upload_meta['Key'],
            UploadId=upload_meta['UploadId'],
            MultipartUpload=part_dict
        )
        return response

    def __abort_multipart_upload(self, upload_meta):
        self.s3_client.abort_multipart_upload(
            Bucket=upload_meta['Bucket'],
            Key=upload_meta['Key'],
            uploadId=upload_meta['uploadId'],
        )

    def __upload_multiPart_custom(self, origin_path, dest_path, chunk_size_mib=1024, tmp_dir=None, validate=True):
        """
        :param chunk_size: size of each chunk in MiB
        """
        chunks = []

        if 's3' not in dest_path:
            raise Exception('Destination not S3 bucket')

        # Init multi-part upload
        multipart_meta = self.__start_multipart_upload(origin_path, dest_path)

        # Performing the next actions in a try block because
        # if something breaks we want to clean up in the exception
        try:
            # == Split & Upload Loop ==
            # Set chunk temporary directory
            if tmp_dir is None:
                tmp_dir = os.path.dirname(origin_path)

            file_size = get_fileSize(origin_path)
            read_pos = 0
            chunk_part = 1
            chunk_size_bytes = chunk_size_mib * 1024 * 1024
            chunk_basename = os.path.splitext(os.path.basename(origin_path))[0]

            with open(origin_path, 'rb') as reader:
                while read_pos < file_size:
                    # Update chunk name (path)
                    chunk_path = os.path.join(tmp_dir, '{}_chunk_{}'.format(chunk_basename, chunk_part))
                    # Create a chunk
                    chunk = Chunk(chunk_path, chunk_part, chunk_size_bytes=chunk_size_bytes)
                    chunk.create_chunk(reader)
                    chunks.append(chunk)
                    print('uploading chunk {}'.format(chunk_part))
                    self.__upload_multipart_part(multipart_meta, chunk)
                    # update read position and chunk part
                    read_pos += chunk_size_bytes
                    chunk_part += 1

            # == Complete Upload ==
            multipart_complete_meta = self.__complete_multipart_upload(multipart_meta, chunks)
        except:
            # Cleanup multipart upload if exception
            self.__abort_multipart_upload(multipart_meta)

        # verify integrity of uploaded file
        if self.__isValid_multipart(chunks, multipart_complete_meta['ETag'][1:-1]):
            return True
        else:
            return False

    def __get_etag(self, s3_path):
        return self.s3_client.head_object(Bucket=self.__get_bucketname(s3_path), Key=self.__get_file_key(s3_path))[
                   'ETag'][1:-1]

    @staticmethod
    def __isValid_multipart(chunks, etag):
        """
        Unofficially the MD5 of the whole multi-part is
        take the MD5 of the parts and concat them together
        tail with "-#ofParts"
        """
        # concat md5 of chunks
        chunk_md5s = map((lambda x: x.md5_bytes), chunks)
        concat_md5s = b''.join(chunk_md5s)
        # get md5 of chunk md5s
        md5_of_chunks = hashlib.md5(concat_md5s).hexdigest()
        # check match
        if md5_of_chunks == etag.split('-')[0]:
            return True
        else:
            return False

    @staticmethod
    def __get_bucketname(bucket_path):
        return urlparse(bucket_path).netloc

    @staticmethod
    def __get_file_key(bucket_path):
        return urlparse(bucket_path).path[1:]


class ChunkStatus(Enum):
    PLANNED = 0
    CREATED = 1
    UPLOADED = 2
    CLEANED = 3


class Chunk:
    def __init__(self, path, part_number, chunk_size_bytes=1073741824):
        self.path = path
        self.status = ChunkStatus.PLANNED
        self.size = int(chunk_size_bytes)
        self.part_number = part_number
        self.md5 = None  # local md5
        self.md5_bytes = None
        self.etag = None  # AWS md5 (unofficially)

    def create_chunk(self, origin_path, memory_size_bytes=104857600):
        # 1MiB * 1024KiB/1MiB * 1024 B/1KiB
        # tmp_dir = os.path.dirname(reader.name)

        with open(self.path, 'wb') as writer:
            for ibytes in range(0, self.size, int(memory_size_bytes)):
                # figure out how much to read
                next_read_bytes = min(memory_size_bytes, (self.size - ibytes))
                # Grab the next bytes
                with open(origin_path, 'rb') as reader:
                    bitbytes = reader.read(next_read_bytes)
                # Write the bytes
                writer.write(bitbytes)

                # Get md5 of chunk
        self.md5 = get_md5(self.path)
        self.md5_bytes = get_md5(self.path, returnHex=False)
        # change status
        self.status = ChunkStatus.CREATED

    def destroy(self):
        os.remove(self.path)

    def validate(self, etag):
        self.etag = etag
        if etag == self.md5:
            return True
        else:
            return False


def get_fileSize(file_path):
    """
    Get the size of a file and return in Bytes
    :param file_path:
    :return: size of file in Bytes
    """
    # file size bytes
    return os.path.getsize(file_path)


def get_md5(file_path, chunk_size=104857600, returnHex=True):
    with open(file_path, "rb") as reader:
        file_hash = hashlib.md5()
        while chunk := reader.read(chunk_size):
            file_hash.update(chunk)

    if returnHex:
        return file_hash.hexdigest()
    else:
        return file_hash.digest()
