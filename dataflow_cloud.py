import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run_pipeline(argv=None):
    # Define pipeline options
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Run on Dataflow
        project='just-camera-432415-h9',  # GCP Project ID
        region='us-central1',  # Region, e.g., us-central1
        temp_location='gs://001project/temp',  # GCS bucket for temporary files
        staging_location='gs://001project/staging',  # GCS bucket for staging files
        job_name='dataflow-job-test',  # Unique job name
    )

    # Create a Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        lines = (p
                 | 'ReadFromText' >> beam.io.ReadFromText('gs://001project/ajay.txt')
                 | 'UpperCase' >> beam.Map(lambda x: x.upper())
                 | 'WriteToText' >> beam.io.WriteToText('gs://001project/output11.txt'))

if __name__ == '__main__':
    run_pipeline()
