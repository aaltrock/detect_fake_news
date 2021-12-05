"""
01 Benchmark throughput local direct runner for stage 1 of the data pipeline (using docker)
"""

import argparse
import logging
import os
import json
import datetime
from google.cloud import storage
import yaml
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery
from google.oauth2 import service_account
import hashlib
import pandas as pd
import re


def run(argv=None, save_main_session=True):
    # Reset experiment by moving the processed files back to the original folder
    processed_path = 'gs://src_fake_news_bs/added/'
    to_process_path = 'gs://src_fake_news_bs/to_add/'

    # Based on https://cloud.google.com/storage/docs/listing-objects#code-samples
    # Based on https://cloud.google.com/storage/docs/listing-objects#code-samples
    def list_blobs(bucket_name):
        """Lists all the blobs in the bucket."""
        # bucket_name = "your-bucket-name"
        storage_client = storage.Client()
        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(bucket_name)
        return [blob.name for blob in blobs]

    # Move the parsed files (data and YAML config file) to the processed folder in GCP cloud storage
    # Ref: https://cloud.google.com/storage/docs/copying-renaming-moving-objects#storage-move-object-python
    def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
        """Moves a blob from one bucket to another with a new name."""
        storage_client = storage.Client()

        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )
        source_bucket.delete_blob(blob_name)

        print(
            "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

    # Authenticate to GCP
    # Set environment variable to denote the location of the JSOn holding the authentication key
    # key_json_path = known_args.json_key_path
    json_key_path = './fake-news-bs-detector-62e838f6b99c.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_key_path

    # Set Docker image location
    docker_image_url = 'gcr.io/fake-news-bs-detector/europe-west2-docker.pkg.dev/fake-news-bs-detector/dataflow-docker-stage-01:latest'

    # No. of iterations to run per sample size
    repeat_nr = 5

    performance_res_ls = []
    for sample_sz in [1250, 2500, 5000, 7500, 10000]:
        for i in range(repeat_nr):
            source_bucket_name = 'src_fake_news_bs'
            source_bucket_path = 'added'
            processed_bucket_success = 'to_add'

            parquet_file_nm = 'gs://src_fake_news_bs/to_add/risdal_' + str(sample_sz) + '.parquet'
            yaml_file_nm = 'gs://src_fake_news_bs/to_add/risdal_' + str(sample_sz) + '.yaml'
            bigquery_output_table = 'src_fake_news_local_docker' + str(sample_sz)
            # param_str = '--region europe-west2 --input=' + parquet_file_nm + ' --output=gs://src_fake_news_bs --runner=PortableRunner --job_endpoint=embed --project=fake-news-bs-detector --staging_location=gs://src_fake_news_bs/staging --temp_location=gs://src_fake_news_bs/tmp/ --template_location=gs://src_fake_news_bs/template1 --requirements_file=./requirements_01_gcp_bucket_to_bigquery_dataflow.txt --source_bucket_name=src_fake_news_bs --source_bucket_base_path=to_add --processed_bucket_success=added --bigquery_output_dataset=fake_news --bigquery_output_table=' + bigquery_output_table + ' --json_key_path=' + json_key_path
            param_str = '--region europe-west2 --input=' + parquet_file_nm + ' --output=gs://src_fake_news_bs --runner=PortableRunner --job_endpoint=embed --project=fake-news-bs-detector --staging_location=gs://src_fake_news_bs/staging --temp_location=gs://src_fake_news_bs/tmp/ --source_bucket_name=src_fake_news_bs --source_bucket_base_path=to_add --processed_bucket_success=added --bigquery_output_dataset=fake_news --bigquery_output_table=' + bigquery_output_table + ' --json_key_path=' + json_key_path + ' --environment_type=DOCKER --environment_config=' + docker_image_url

            # If exists, move the processed file back to the 'to-add' folder before starting the pipeline.
            files_ls = list_blobs(source_bucket_name)
            pattern = r'^' + source_bucket_path + '\/\w{1,}'
            files_ls = [file_nm for file_nm in files_ls if
                        re.search(pattern, file_nm) is not None]  # Filter for files in the ingestion folder

            # Move the processed files to the folder to the ingestion folder
            for file_nm in files_ls:
                dest_file_nm = file_nm.replace(source_bucket_path + '/',
                                               processed_bucket_success + '/')
                move_blob(source_bucket_name, file_nm, source_bucket_name, dest_file_nm)


            start_tm = datetime.datetime.now()

            # Execute the Python script for the Stage 1 pipeline with the parameters defined above
            os.system('../venv/bin/python 01_gcp_bucket_to_bigquery_dataflow.py ' + param_str)

            # Calculate the duration
            end_time = datetime.datetime.now()
            duration = end_time - start_tm

            # Pack results into the list
            performance_res_ls += [(sample_sz, i, start_tm, end_time, duration)]

            # Get the list of files that exist in the import folder
            files_ls = list_blobs(source_bucket_name)
            pattern = r'^' + source_bucket_path + '\/\w{1,}'
            files_ls = [file_nm for file_nm in files_ls if
                        re.search(pattern, file_nm) is not None]  # Filter for files in the ingestion folder

            # Move the processed files to the folder to the ingestion folder
            for file_nm in files_ls:
                dest_file_nm = file_nm.replace(source_bucket_path + '/',
                                               processed_bucket_success + '/')
                move_blob(source_bucket_name, file_nm, source_bucket_name, dest_file_nm)

    # At the end of the run, transform the list of results into data frame
    fields_nm_ls = ['Sample Size', 'Iteration', 'Start Time', 'End Time', 'Duration']
    benchmark_res_df = pd.DataFrame(performance_res_ls, columns=fields_nm_ls)
    benchmark_res_df.to_csv('direct_docker_runner_benchmark_res_df.csv', index=False)

    print('END OF BENCHMARKING')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
