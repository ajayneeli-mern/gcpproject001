import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

def run_pipeline(argv=None):
    # Define pipeline options
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Run on Dataflow
        project='just-camera-432415-h9',  # GCP Project ID
        region='asia-east1',  # Region, e.g., us-central1
        temp_location='gs://001project/temp',  # GCS bucket for temporary files
        staging_location='gs://001project/staging',  # GCS bucket for staging files
        job_name='dataflow-job-csv-to-bigquery1',  # Unique job name
        save_main_session=True,  # Required for Dataflow
    )

    # Create a Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        lines = (p
                 | 'ReadFromCSV' >> beam.io.ReadFromText('gs://001project/example.csv', skip_header_lines=1)  # Read CSV file, skip header
                 | 'ParseCSV' >> beam.Map(lambda line: dict(zip(['name', 'age', 'salary'], line.split(','))))  # Adjust column names and split
                 | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                     table='just-camera-432415-h9:001projectbigquery.employees',  # BigQuery table
                     schema='name:STRING,age:STRING,salary:STRING',  # Schema of the BigQuery table
                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,  # Write mode
                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED  # Table creation mode
                 ))

if __name__ == '__main__':
    run_pipeline()
