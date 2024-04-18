import logging
from google.cloud import storage
 

def getheader(filename,bucket=None):
        logging.info('Filename is : %s',str(filename))
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.blob('pipelines/gcs-to-bq/temp/header.txt')
        blob = blob.download_as_text()
        lines = blob.split('\n')
        logging.info('Header is : %s',lines[0]) 

        return lines[0]
