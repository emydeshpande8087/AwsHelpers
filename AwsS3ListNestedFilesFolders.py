#DEFINE IMPORTS
from LogAgent import get_logger
import boto3,os
from boto3.s3.transfer import TransferConfig

#DEFINE GLOBAL VARIABLES,CLIENTS AND LOG OBJECTS
util_log = get_logger(__name__)  # Get a logger for the utility function module
s3=boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
s3BucketName=os.environ.get("S3_BUCKET_NAME")
s3_transfer_config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                        multipart_chunksize=1024*25, use_threads=True)




def get_list_of_files_from_s3(p_input_folder):
    try:    
        folder_name=p_input_folder+"/"
        #fetch list of files available and display count
        file_list = []
        #using head object to identify if the folder/bucket really do exist
        #if folder doesnt exist , this will throw the exception.
        head_object_response = s3.head_object(Bucket=s3BucketName, Key=folder_name)
        #using paginator to handle large number of files 
        for page in paginator.paginate(Bucket=s3BucketName,Prefix=folder_name):
            for content in page.get('Contents', []):
                if not content['Key'].endswith('/'):
                    one_file=content['Key']
                    file_list.append((one_file))
                    
        util_log.info("List of Files present: %s",file_list)
        return file_list
    except Exception as e:
        util_log.error(f"Input Folder '{folder_name}' not found in S3 bucket '{s3BucketName}'.", exc_info=True)
        raise
