from optparse import OptionParser
import sys
import traceback

from S3Streamer import S3Streamer


# TODO: Make an entry point?
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
    streamer = S3Streamer(region='us-east-1')
    try:
        size, chunk_number, upload_response = streamer.upload_to_s3(s3_bucket, s3_key, url)
    except Exception as exc:
        print('Caught exception, aborting multipart upload')
        traceback.print_exc()

    print('Moved {} bytes in {} chunks'.format(size, chunk_number))


if __name__== '__main__':
    main()