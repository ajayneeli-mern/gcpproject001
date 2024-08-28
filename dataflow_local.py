import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run_pipeline(argv=None):
    # Define pipeline options
    pipeline_options = PipelineOptions(
        runner='DirectRunner',  # Run locally with DirectRunner
        temp_location='gs://001project/temp',  # Temporary GCS location for intermediate files
        project='just-camera-432415-h9',  # GCP Project ID
    )

    # Create a Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        lines = (p
                 | 'ReadFromText' >> beam.io.ReadFromText('local_input.txt')
                 | 'UpperCase' >> beam.Map(lambda x: x.upper())
                 | 'WriteToText' >> beam.io.WriteToText('local_output'))

if __name__ == '__main__':
    run_pipeline()
