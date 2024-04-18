import logging
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from apache_beam import pvalue
from options import MyPipelineOptions
from processors import convert_to_json
from processors import getHeader
from processors import OutputValueProviderFn
from processors import errors
import sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'test'))
import test_convert_to_json
import unittest

 #only calling statements should be in here

if __name__ == '__main__':

	logging.getLogger().setLevel(logging.INFO)
	suite = unittest.TestLoader().loadTestsFromModule(test_convert_to_json)
	result_unittest = unittest.TextTestRunner().run(suite)

	if result_unittest.wasSuccessful():

		pipeline = beam.Pipeline(options=PipelineOptions())

		#Reading runtime parameters

		runtime_options = PipelineOptions().view_as(MyPipelineOptions.RuntimeOptions)

		

		pcollection = pipeline | 'Create Empty Pcollection' >> beam.Create([None])		

		#Reading file names from runtime

		file_list = pcollection | 'Get FileName from Runtime' >> beam.ParDo(OutputValueProviderFn.OutputValueProviderFn(runtime_options.inputfile))

		#Getting the header

		header = pcollection | 'Get header' >> beam.Map(getHeader.getheader,runtime_options.bucket)
        files = file_list | 'Read File From Runtime' >> beam.io.ReadAllFromText(skip_header_lines=1)

		#Converting CSV file to JSON

		json_data = files | 'Convert To JSON' >> beam.ParDo(convert_to_json.convert_to_json(),pvalue.AsSingleton(header),runtime_options.error_table).with_outputs('exception_row','validated_row')

		

		#Writing the output to bq

		output = json_data['validated_row'] | 'Write to BQ' >> beam.io.WriteToBigQuery(table=runtime_options.desttable,create_disposition='CREATE_NEVER',write_disposition='WRITE_APPEND',ignore_unknown_columns=True,method='STREAMING_INSERTS',insert_retry_strategy="RETRY_NEVER")

		

		#Adding errors to a dataframe
        schema_error=output['FailedRows'] | beam.ParDo(errors.Errors(),runtime_options.error_table)

		full_error_set=((schema_error,json_data['exception_row']) | 'Merge Errors' >> beam.Flatten())

		failure = full_error_set | 'print failed rows' >> beam.Map(print)

		#Writing Errors to Error table

		errors = full_error_set | 'Write Error to BQ' >> beam.io.WriteToBigQuery(str(runtime_options.error_table),ignore_unknown_columns=True,create_disposition='CREATE_NEVER',write_disposition='WRITE_APPEND',method='STREAMING_INSERTS')

 

		#Start Pipeline

		result = pipeline.run()
