import argparse
import csv
import json
from io import StringIO

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.filesystems import FileSystems


class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        """
        element: bytes from Pub/Sub
        returns: file path string 'gs://bucket/object'
        """
        payload = json.loads(element.decode("utf-8"))
        bucket = payload["bucket"]
        name = payload["name"]
        gcs_path = f"gs://{bucket}/{name}"
        yield gcs_path


class ReadCsvFromGCS(beam.DoFn):
    def process(self, gcs_path):
        """
        gcs_path: 'gs://bucket/path/file.csv'
        yield: dict rows for BigQuery
        """
        with FileSystems.open(gcs_path) as f:
            text = f.read().decode("utf-8")

        # use csv reader so header handled
        reader = csv.DictReader(StringIO(text))
        for row in reader:
            # row like {"id": "123", "name": "...", "location": "..."}
            yield {
                "id": int(row["id"]),
                "name": row["name"],
                "location": row["location"],
            }


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_subscription",
        required=True,
        help="Pub/Sub subscription to read from, e.g. projects/PROJECT/subscriptions/gcs-csv-sub",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery table: PROJECT:DATASET.TABLE",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # streaming
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            | "ParseGCSNotification" >> beam.ParDo(ParsePubSubMessage())
            | "ReadCSV" >> beam.ParDo(ReadCsvFromGCS())
            # optional: deduplicate by id
            # | "KeyById" >> beam.Map(lambda row: (row["id"], row))
            # | "DistinctById" >> beam.GroupByKey()
            # | "TakeOnePerId" >> beam.Map(lambda kv: kv[1][0])
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema="id:INTEGER,name:STRING,location:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()
