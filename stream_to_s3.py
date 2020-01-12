import boto3
import requests
from tqdm import tqdm

from optparse import OptionParser
import sys
import traceback

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
    part_list = []
    try:
        # TODO: In testing with a ~180MB file and a 1MB chunk_size it only ran for 23 iterations.
        # That math doesn't work out; probably need to look at what's going on there.
        for chunk_number, content_chunk in enumerate(tqdm(req.iter_content(chunk_size=FIVE_MB_IN_BYTES))):
            part_upload_response = s3_client.upload_part(Body=content_chunk, Bucket=s3_bucket, Key=s3_key, UploadId=upload_id, PartNumber=chunk_number + 1)
            part_list.append({
                'PartNumber': chunk_number + 1,
                'ETag': part_upload_response['ETag']
            })
            size += len(content_chunk)
    except Exception as exc:
        print('Caught exception, aborting multipart upload')
        traceback.print_exc()
        s3_client.abort_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id)


    part_info = {
        'Parts': part_list
    }
    print(s3_client.complete_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id, MultipartUpload=part_info))

    print('Moved {} bytes in {} chunks'.format(size, chunk_number))





if __name__== '__main__':
    main()