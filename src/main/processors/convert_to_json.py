import json
import csv
import logging
import apache_beam as beam
from apache_beam import pvalue
class convert_to_json(beam.DoFn): 

        def process(self,element,headers,error):

                logging.getLogger().setLevel(logging.INFO)

                project_name=error.split(":")[0]

                header_list = list(csv.reader([headers],quotechar='"', delimiter=','))

                header_data = header_list[0]

                element_list =  list(csv.reader([element],quotechar='"', delimiter=','))

                element_data = element_list[0]

                logging.info('Elements in Headers : %d',len(header_data))

                logging.info('Elements in Data : %d',len(element_data))

                jsondict={}
                if len(header_data) == len(element_data):

                        for i in range(len(header_data)):

                                if str(element_data[i]) != '':

                                        jsondict[str(header_data[i])] = str(element_data[i])

                        yield pvalue.TaggedOutput('validated_row',jsondict)

                else:

                        logging.getLogger().setLevel(logging.ERROR)

                        exception={}

                        exception['input_file_name']="event_store"

                        exception['target_table_name']="event_store"

                        exception['error_message']="Header and Data columns do not match"

                        exception['event_data']=str(element)

                        logging.error("ALERT for PROJECT ID :"+ project_name +", JOB NAME: gcs-to-bq-fdr-event-store %s in %s", exception['error_message'],exception['event_data'])

                        yield  pvalue.TaggedOutput('exception_row',exception)
