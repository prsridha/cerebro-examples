import glob
from boto3 import Session
import os
import sys
from multiprocessing import Pool

# target location of the files on S3
S3_BUCKET_NAME = 'cerebro-imagenet-ca'
S3_FOLDER_NAME = 'Annotations'

# Source location of files on local system 
DATA_FILES_LOCATION   = './Annotations'
s3 = None

def upload(filepath):
    filename = "/".join(filepath.split("/")[-2:])
    s3_file = os.path.join(S3_FOLDER_NAME, filename)
    uploaded = s3.upload_file(filepath, S3_BUCKET_NAME, s3_file)
    print("Uploaded file", filename)

def main():
    global s3

    session = Session(profile_name="default")
    s3 = session.client('s3')
    # filepaths = glob.glob(DATA_FILES_LOCATION + "/*")
    filepaths = []
    for dirpath,_,filenames in os.walk(DATA_FILES_LOCATION):
        for f in filenames:
            p = os.path.abspath(os.path.join(dirpath, f))
            filepaths.append(p)

    p = Pool(processes=50)
    p.map(upload, filepaths)

    print("All Data files uploaded to S3 Ok")

main()