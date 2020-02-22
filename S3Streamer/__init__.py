import boto3
import requests

DEFAULT_CHUNK_SIZE = FIVE_MB_IN_BYTES = 5 * 1024 * 1024 # Minimum size for an individual part in a multipart upload

class S3StreamerException(Exception): pass

class S3Streamer(object):

    def __init__(self, region, aws_access_key_id=None, aws_secret_access_key=None):
        if (aws_access_key_id or aws_secret_access_key) and not all([aws_access_key_id, aws_secret_access_key]):
            raise S3StreamerException('Both aws_access_key_id and aws_secret_access_key must be specified!')

        if aws_access_key_id and aws_secret_access_key:
            self.client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        else:
            self.client = boto3.client('s3', region_name=region)

    def upload_to_s3(self, s3_bucket, s3_key, url, chunk_size=DEFAULT_CHUNK_SIZE):
        if chunk_size < DEFAULT_CHUNK_SIZE:
            raise S3StreamerException('Minimum size for chunk is {} for S3 multipart uploads'.format(DEFAULT_CHUNK_SIZE))
        req = requests.request('GET', url, stream=True)
        multipart_upload_response = self.client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
        upload_id = multipart_upload_response['UploadId']
        part_list = []
        size = 0
        try:
            # TODO: In testing with a ~180MB file and a 1MB chunk_size it only ran for 23 iterations.
            # That math doesn't work out; probably need to look at what's going on there.
            for chunk_number, content_chunk in enumerate(req.iter_content(chunk_size=FIVE_MB_IN_BYTES)):
                part_upload_response = self.client.upload_part(Body=content_chunk, Bucket=s3_bucket, Key=s3_key,
                                                             UploadId=upload_id, PartNumber=chunk_number + 1)
                part_list.append({
                    'PartNumber': chunk_number + 1,
                    'ETag': part_upload_response['ETag']
                })
                size += len(content_chunk)
        except Exception as exc:
            self.client.abort_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id)
            raise S3StreamerException('Caught exception {} performing upload, cancelling multipart upload {}'.format(exc, upload_id))

        part_info = {
            'Parts': part_list
        }
        return size, chunk_number, self.client.complete_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id,
                                                                         MultipartUpload=part_info)
