import boto3
import requests
from tqdm import tqdm

from optparse import OptionParser
import sys

# This is the minimum size for each part of a multipart upload
FIVE_MB_IN_BYTES = 5 * 1000 * 1000

def main():
    parser = OptionParser()
    parser.add_option("-u", "--url",
                      action="store",
                      dest="url",
                      default=None,
                      help="URL to download")
    parser.add_option("-s", "--s3bucket",
                      action="store",
                      dest="s3_bucket",
                      default=None,
                      help="S3 bucket")
    parser.add_option("-k", "--s3key",
                      action="store",
                      dest="s3_key",
                      default=None,
                      help="S3 key")

    options, args = parser.parse_args()
    s3_bucket = options.s3_bucket
    s3_key = options.s3_key
    url = options.url
    if not all([url, s3_bucket, s3_key]):
        print('S3 Destination and URL required - exiting')
        sys.exit(1)

    print('Saving {} to {}/{}'.format(url, s3_bucket, s3_key))
    s3_client = boto3.client('s3', region_name='us-east-1')
    req = requests.request('GET', url, stream=True)
    size = 0
    multipart_upload_response = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
    upload_id = multipart_upload_response['UploadId']
    try:
        for chunk_number, content_chunk in enumerate(tqdm(req.iter_content(chunk_size=FIVE_MB_IN_BYTES))):
            s3_client.upload_part(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id, PartNumber=chunk_number)
            size += len(content_chunk)
    except Exception:
        print('Caught exception, aborting multipart upload')
        s3_client.abort_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id)

    s3_client.complete_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id)

    print('Moved {} bytes'.format(size))





if __name__== '__main__':
    main()