import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam import Create, Map
import json
from apache_beam.options.pipeline_options import GoogleCloudOptions
import google.auth
from apache_beam.runners import DataflowRunner

INPUT_SUBSCRIPTION = "projects/blissful-ly/subscriptions/healthkit-sub"
BIGQUERY_TABLE = "blissful-ly.healthkit.user_health_data"
SCHEMA = {"fields":[{"name":"uid","type":"STRING","mode":"REQUIRED"},{"name":"start_time","type":"TIMESTAMP","mode":"NULLABLE"},{"name":"end_time","type":"TIMESTAMP","mode":"NULLABLE"},{"name":"health_data","type":"RECORD","mode":"REPEATED","fields":[{"name":"key","type":"STRING","mode":"REQUIRED"},{"name":"value","type":"RECORD","mode":"NULLABLE","fields":[{"name":"int_value","type":"INTEGER","mode":"NULLABLE"},{"name":"float_value","type":"FLOAT","mode":"NULLABLE"},{"name":"string_value","type":"STRING","mode":"NULLABLE"},{"name":"bool_value","type":"BOOLEAN","mode":"NULLABLE"},{"name":"time_series","type":"RECORD","mode":"REPEATED","fields":[{"name":"timestamp","type":"TIMESTAMP","mode":"NULLABLE"},{"name":"value","type":"RECORD","mode":"NULLABLE","fields":[{"name":"int_value","type":"INTEGER","mode":"NULLABLE"},{"name":"float_value","type":"FLOAT","mode":"NULLABLE"},{"name":"string_value","type":"STRING","mode":"NULLABLE"},{"name":"bool_value","type":"BOOLEAN","mode":"NULLABLE"}]}]}]}]}]}

options = pipeline_options.PipelineOptions(flags={})

_, options.view_as(GoogleCloudOptions).project = google.auth.default()

options.view_as(GoogleCloudOptions).region = 'us-central1'

dataflow_gcs_location = 'gs://blissful-ly.appspot.com/healthkit/dataflow'

options.view_as(GoogleCloudOptions).staging_location = '%s/staging' % dataflow_gcs_location

options.view_as(GoogleCloudOptions).temp_location = '%s/temp' % dataflow_gcs_location

options.view_as(pipeline_options.StandardOptions).streaming = True

pipeLine = beam.Pipeline(options=options)
process = (
    pipeLine
    | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
        subscription=INPUT_SUBSCRIPTION)
    | Map(json.loads)
    | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
        BIGQUERY_TABLE,
        schema = SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    )
)

runner = DataflowRunner()
runner.run_pipeline(pipeLine, options = options)