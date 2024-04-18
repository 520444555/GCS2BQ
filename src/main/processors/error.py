import json
import csv
import logging
import apache_beam as beam 

class Errors(beam.DoFn):

        def process(self,element,error):

            logging.getLogger().setLevel(logging.ERROR)

            project_name=error.split(":")[0]

            exception={}

            exception['input_file_name']="event_store"

            exception['target_table_name']="event_store"

            exception['error_message']="Schema Validation error"

            exception['event_data']=str(element)

            logging.error("ALERT for PROJECT ID :"+ project_name +", JOB NAME: gcs-to-bq %s in %s", exception['error_message'],exception['event_data'])

            yield  exception
