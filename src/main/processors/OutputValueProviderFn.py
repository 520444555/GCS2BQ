import apache_beam as beam
import logging

class OutputValueProviderFn(beam.DoFn):
        def __init__(self,filenames):
                self.filenames=filenames


        def process(self,unused_elm):

                logging.info('Runtime Filename is : %s',self.filenames.get())
                logging.info('Runtime Filename type is : %s',type(self.filenames.get()))
                files=self.filenames.get().split(',')
                for file in files:
                        yield file
