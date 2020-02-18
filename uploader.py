import os
import zipfile
import boto3
from botocore.exceptions import ClientError

import functions
import config

# Compression to ZIP


def make_zipfile(file_path):
    functions.log('### COMPRESSION START ###')
    output_filename = file_path + '.zip'

    with zipfile.ZipFile(output_filename, 'w') as _z:
        _z.write(file_path, os.path.basename(file_path),
                 compress_type=zipfile.ZIP_DEFLATED)

    functions.log('### COMPRESSION SUCCESS ###')
    functions.log('### SIZE : ' + str(os.path.getsize(output_filename)))

    return output_filename

# Upload CSV file to AWS S3 after compression as ZIP


def uploadToS3(filetype, filepath, serial, dateinfo, filename, bucket, divider):
    try:
        # Compression
        # output_filename: ZIP file full path
        output_filename = make_zipfile(filepath)

        # Split date info from date parmeter
        year = dateinfo[0:4]
        month = dateinfo[4:6]
        date = dateinfo[6:]

        ### Start to upload to AWS S3 ###
        s3 = boto3.client('s3',
                          aws_access_key_id=config.aws_key,
                          aws_secret_access_key=config.aws_secret,
                          region_name=config.aws_region
                          )

        if(filetype == 'ISO' or filetype == 'DIN'):
            s3_fullpath = filetype + divider + serial + divider + \
                year + divider + month + divider + filename + '.zip'
        else:
            s3_fullpath = filetype + divider + serial + divider + year + \
                divider + month + divider + date + divider + filename + '.zip'

        functions.log('remote full path: ' + s3_fullpath)

        functions.log('### UPLOAD START ###')
        s3.upload_file(output_filename, bucket, s3_fullpath)
        ### End uploading to AWS S3 ###

        functions.log('### UPLOAD SUCCESS ###')

        return output_filename

    except Exception as e:
        functions.log(e)
