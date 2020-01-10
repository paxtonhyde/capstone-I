## functions for reading and writing files to and from spark

import os
import boto3
import pyspark as ps
from botocore.exceptions import ClientError

SCRIPT_DIRECTORY = os.path.realpath("")
HOME_DIRECTORY = os.path.split(SCRIPT_DIRECTORY)[0]
DATA_DIRECTORY = os.path.join(HOME_DIRECTORY, "data")

def loaddata(filename, sparkobject, s3bucket):
    path = f'{DATA_DIRECTORY}/{filename}'
    if filename not in os.listdir(DATA_DIRECTORY):
        s3_client = boto3.client('s3')
        s3_client.download_file(s3bucket,\
                        filename,\
                        path)
    extension = filename.split('.')[1]
    return sparkobject.read.load(path, format = extension, header='true')

def uploaddata(sparkdataframe, filename, s3bucket, mode="overwrite"):
    path = f'{DATA_DIRECTORY}/{filename}'
    extension = filename.split('.')[1]
    sparkdataframe.write.save(path=path, format=extension, mode=mode)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(path, s3bucket, filename)
    except ClientError:
        print(f"Failed uploading {filename} to bucket {s3bucket}.")
        return False
    
    return True
