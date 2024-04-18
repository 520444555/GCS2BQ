import unittest
import xmlrunner
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam import pvalue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import is_not_empty
from apache_beam.testing.util import is_empty
import sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'main'))
from processors import convert_to_json

class convert_to_json_test(unittest.TestCase):
    def test_valid_input_data(self):
            header = ["column1,columns2"]
            input_data = ["column1sampledata","column2sampledata"]
            # with beam.Pipeline() as p:
            pcoll = p | 'Create input' >> beam.Create(input_data)
            header = p | 'Create header' >> beam.Create(header)
            json_data = pcoll | 'Convert to JSON' >> beam.ParDo(convert_to_json.convert_to_json(),pvalue.AsSingleton(header),"hsbc-10534429-fdreu-dev:FZ_EU_FDR_DEV.event_store_errors").with_outputs('exception_row','validated_row')
           
            assert_that(json_data['validated_row'],is_not_empty(),label='test_valid_input_data_valid')

            assert_that(json_data['exception_row'], is_empty(), label='test_valid_input_data_exception')

    def test_invalid_input_data(self):

            header = [""]

            input_data = [""]

            with TestPipeline() as p:
             # with beam.Pipeline() as p:
            pcoll = p | 'Create input' >> beam.Create(input_data)

            header = p | 'Create header' >> beam.Create(header)

            json_data = pcoll | 'Convert to JSON' >> beam.ParDo(convert_to_json.convert_to_json(),pvalue.AsSingleton(header),"hsbc-10534429-fdreu-dev:FZ_EU_FDR_DEV.event_store_errors").with_outputs('exception_row','validated_row')
 
 

            assert_that(json_data['validated_row'],is_empty(),label='test_invalid_input_data_valid')

            assert_that(json_data['exception_row'], is_not_empty(),label='test_invalid_input_data_exception')

    def test_no_input_data(self):

            header = [""]
            input_data = []
            with TestPipeline() as p:

        # with beam.Pipeline() as p:
                pcoll = p | 'Create input' >> beam.Create(input_data)
                assert_that(pcoll, is_empty(), label='test_no_input_data')
                header = p | 'Create header' >> beam.Create(header)
                json_data = pcoll | 'Convert to JSON' >> beam.ParDo(convert_to_json.convert_to_json(),pvalue.AsSingleton(header),"hsbc-10534429-fdreu-dev:FZ_EU_FDR_DEV.event_store_errors").with_outputs('exception_row','validated_row')

if __name__ == '__main__':
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output='utest-reports'),
        failfast=False,
        buffer=False,
        catchbreak=False)
