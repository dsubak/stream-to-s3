# stream-to-s3
Streams a remote file to a given S3 bucket with a constant memory footprint.

# Usage
This is not currently packaged, in the interim, install from GitHub with the following command
```
pip install git+git://github.com/dsubak/stream-to-s3.git#egg=S3Streamer_dsubak
```
 
```    
from S3Streamer import S3Streamer
streamer = S3Streamer(region='us-east-1')
size, chunk_number, upload_response = streamer.upload_to_s3(s3_bucket, s3_key, url)
```
