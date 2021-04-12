#Pub/Sub to GCS Account Microservice Pet Created pipeline

#Import Packages
import logging
import json
import traceback

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options import pipeline_options
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io import WriteToText

from apache_beam.runners import DataflowRunner

import google.auth

from apache_beam.io.textio import ReadFromText
from pandas.io.json import json_normalize
import pandas as pd
import apache_beam.io.textio as TextIO
from apache_beam.coders import coders
from apache_beam.transforms.core import CombineFn
import time
from datetime import datetime
from apache_beam.io.filesystems import FileSystems

class CustomPipelineOptions(PipelineOptions):
    """
    Runtime Parameters given during template execution
    path and organization parameters are necessary for execution of pipeline
    campaign is optional for committing to bigquery
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--path',
            type=str,
            help='Path of the file to read from',
            default = 'dev-analytics-data-lake/Accounts/PetCreated/PetCreated_testing.json')
        parser.add_value_provider_argument(
            '--output',
            type=str,
            help='Output file if needed')

project = 'furlong-platform-sbx-8d14f3'

#Pipeline Logic
def streaming_pipeline(project, region="us-central1"):
    
    subscription = "projects/furlong-platform-sbx-8d14f3/subscriptions/Pet_Created"
    bucket = "gs://dev-analytics-data-lake/Accounts/Pet_Created/"
    
    options = PipelineOptions(
        streaming=True,
        project=project,
        region=region,
        # Make sure staging and temp folder are created using cloud commands
        staging_location="gs://dev-analytics-temp-files/staging",
        temp_location='gs://dev-analytics-temp-files/temp',
        template_location = 'gs://dev-analytics-temp-files/Accounts/AM_PetCreated_PStoGCS.py',
        autoscaling_algorithm = 'THROUGHPUT_BASED',
        max_num_workers = 5
    )

    p = beam.Pipeline(DataflowRunner(), options=options)


    class normalize(beam.DoFn):
        def process(self, element):
            import pandas as pd
            import json
            import time
            from datetime import datetime
            from apache_beam.io.filesystems import FileSystems
            x = json.loads(element.decode("utf8"))
            x = pd.json_normalize(x, max_level = 0)
            x = x.to_dict('r')
            
            return [x]
    
    class WriteToGCS(beam.DoFn):
        def __init__(self):
            self.outdir = "gs://dev-analytics-data-lake/Accounts/PetCreated/"

        def process(self, element):
            import json
            import time
            from datetime import datetime
            from apache_beam.io.filesystems import FileSystems
            dateTimeObj = datetime.now()
            timestampStr = dateTimeObj.strftime("%m-%d-%Y-%H:%M")
            file_prefix = "Pet_Created_Output_" + timestampStr + '.json' 
            writer = FileSystems.create(self.outdir + file_prefix, 'text/plain')
            writer.write(element.encode())
            writer.close()

    subscription = "projects/furlong-platform-sbx-8d14f3/subscriptions/Pet_Created"
    topic = "projects/furlong-platform-sbx-8d14f3/topics/pet-created"
    bucket = "gs://dev-analytics-data-lake/Accounts/PetCreated/"

    lines = (p | "Read Topic" >> ReadFromPubSub(subscription = subscription)
            | "Normalize into DF" >> beam.ParDo(normalize())
            | "WriteOutput" >> beam.ParDo(WriteToGCS())
            )

    return p.run()

try:
    pipeline = streaming_pipeline(project)
    print("\n PIPELINE RUNNING \n")
except (KeyboardInterrupt, SystemExit):
    raise
except:
    print("\n PIPELINE FAILED")
    traceback.print_exc()

