from apache_beam.options.pipeline_options import PipelineOptions 

class RuntimeOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
                parser.add_argument("--error_table")
                parser.add_argument("--bucket")
                parser.add_value_provider_argument("--inputfile",help="Path of the file to read from")
                parser.add_argument("--desttable",help="Destination Table")
