"""
01 Benchmark throughput local direct runner
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


# DoFn class to parse the columns into standard formats
class ParseCSV(beam.DoFn):
    def process(self, element, bucket_name, bucket_base_path, template_config_dct):
        record_dct = element
        config_dct = None

        # Load in the mapping YAML file
        # Code customised from Google Cloud documentation: https://cloud.google.com/storage/docs/downloading-objects#code-samples
        def download_blob(bucket_name, source_blob_name, destination_file_name):
            """Downloads a blob from the bucket."""
            # The ID of your GCS bucket
            # bucket_name = "your-bucket-name"

            # The ID of your GCS object
            # source_blob_name = "storage-object-name"

            # The path to which the file should be downloaded
            # destination_file_name = "local/path/to/file"

            storage_client = storage.Client()

            bucket = storage_client.bucket(bucket_name)

            # Construct a client side representation of a blob.
            # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
            # any content from Google Cloud Storage. As we don't need additional data,
            # using `Bucket.blob` is preferred here.
            # blob = bucket.blob(source_blob_name)
            blob = bucket.blob(source_blob_name)
            with open(destination_file_name, 'wb') as f:
                # Use TQDM to show progress bar:
                # Ref: https://stackoverflow.com/questions/62811608/how-to-show-progress-bar-when-we-are-downloading-a-file-from-cloud-bucket-using
                # with tqdm.wrapattr(f, 'write', total=blob.size) as file_obj:
                # blob.download_to_filename(destination_file_name)
                storage_client.download_blob_to_file(blob, f)

            # print(
            #     "Downloaded storage object {} from bucket {} to local file {}.".format(
            #         source_blob_name, bucket_name, destination_file_name
            #     )
            # )

        # Download the YAML file to local
        source_yml_file_nm = bucket_base_path + '/' + record_dct.get('file_name') + '.yml'
        dest_yml_file_nm = record_dct.get('file_name') + '.yml'
        try:
            # If config YAML file is not downloaded from GCP cloud storage, download the copy
            if not os.path.exists(dest_yml_file_nm):
                download_blob(bucket_name, source_yml_file_nm, dest_yml_file_nm)

        # If no YAMl file exists or the dictionary for the file config is different to the template then return None
        except Exception as e:
            print(e)
            yield None

        # Open the YAML config file after copy to local
        with open(dest_yml_file_nm, 'r') as f_read:
            config_dct = yaml.safe_load(f_read)

        if config_dct is not None:

            # Return None if no data frame exists
            if record_dct is None and template_config_dct.keys() == config_dct.keys():
                yield None

            # Create a new parsed dict to return
            parse_dct = {'etl_timestamp': datetime.datetime.now()}

            # Mandatory columns
            for key in list(config_dct.get('mandatory_columns').keys()):
                try:
                    col_nm = config_dct.get('mandatory_columns').get(key).get('column_name')
                    if col_nm in list(record_dct.keys()):
                        val = record_dct.get(col_nm)
                        if val is None:
                            val = 'null'
                        if val == '':
                            val = 'null'
                        parse_dct.update({key: val})
                    else:
                        yield None
                except Exception as e:
                    print(e)
                    yield None

            # Supplementary columns
            for key in list(config_dct.get('supplementary_columns').keys()):
                try:
                    col_nm = config_dct.get('supplementary_columns').get(key).get('column_name')
                    if col_nm in list(record_dct.keys()):
                        val = record_dct.get(col_nm)
                        if val is None:
                            val = 'null'
                        if val == '':
                            val = 'null'
                        parse_dct.update({key: val})
                    else:
                        parse_dct.update({key: 'null'})
                except Exception as e:
                    print(e)
                    yield None

            # Custom columns
            if config_dct.get('mandatory_columns') is None:
                parse_dct.update({'custom_columns': {}})
            else:
                __custom_dct = {}
                for col_nm in config_dct.get('custom_columns'):
                    if col_nm in list(record_dct.keys()):
                        __custom_dct.update({col_nm: record_dct.get(col_nm)})
                    else:
                        __custom_dct.update({col_nm: 'null'})

                json_str = json.dumps(__custom_dct)

                parse_dct.update({'custom_columns': json_str})

            # Create hashes for key columns
            for key in ['body', 'url', 'title', 'file_name', 'author']:
                val = parse_dct.get(key)
                if val is None or val == '':
                    val = 'null'
                val = val.encode('utf-8')
                hash = hashlib.sha256(val).hexdigest()
                new_key = key + '_hash'
                parse_dct.update({new_key: hash})

            # Create unique ID based on hashes of title and body (concatenated)
            title_hash = parse_dct.get('title_hash')
            body_hash = parse_dct.get('body_hash')

            parse_dct.update({'article_id': title_hash + body_hash})

            try:
                yield parse_dct
            except Exception as e:
                print(e)
                yield None

        else:
            yield None


# DoFn class to parse the columns into standard formats
class WriteToBigQuery(beam.DoFn):
    def process(self, element, project_id, dataset_nm, table_nm):
        if element is not None:
            # df = pd.DataFrame.from_dict(element, orient='index').T
            destination = dataset_nm + '.' + table_nm
            element.to_gbq(destination_table=destination, project_id=project_id, if_exists='append')
            yield True
        else:
            yield False


# DoFn class to write Turtle strings into TTL text files
class WriteTurtleJSON(beam.DoFn):
    def process(self, element):
        url_hash = element[0]
        ttl_str = element[1]
        url_hash, url, domain, domain_hash, body_hash, body = element[2]
        json_str = element[3]
        output_path_ttl = element[4]
        output_path_json = element[5]

        # Write RDF Graph to Turtle
        print('Outpath Turtle: {}'.format(output_path_ttl))
        writer = fileio.filesystems.FileSystems.create(output_path_ttl)
        writer.write(bytes(ttl_str, encoding='utf-8'))
        writer.close()

        # Write news body to JSON string then write to file
        print('Outpath JSON: {}'.format(output_path_json))
        # json_str = json.dumps(news_body_dct, indent=4)
        writer = fileio.filesystems.FileSystems.create(output_path_json)
        writer.write(bytes(json_str, encoding='utf-8'))
        writer.close()

        # Output the tuple of text body to write separately to TTL file
        yield url, body_hash, body


def run(argv=None, save_main_session=True):
    # Parse parameters
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default=os.path.join(os.getcwd(), 'parse_dict'),
        help='Batched JSON files.')
    parser.add_argument(
        '--bigquery_output_dataset',
        dest='bigquery_output_dataset',
        required=True,
        help='BigQuery output dataset.')
    parser.add_argument(
        '--bigquery_output_table',
        dest='bigquery_output_table',
        required=True,
        help='BigQuery output table within the dataset.')
    parser.add_argument(
        '--processed_bucket_success',
        dest='processed_bucket_success',
        required=True,
        help='GCP cloud storage location where to store parsed files, if successful.')
    # parser.add_argument(
    #     '--temp_location',
    #     dest='temp_location',
    #     required=True,
    #     help='Temp location to hold files during Beam processing.')
    # parser.add_argument(
    #     '--staging_location',
    #     dest='temp_location',
    #     required=True,
    #     help='Staging locations.')
    parser.add_argument(
        '--json_key_path',
        dest='json_key_path',
        required=True,
        help='Location where JSON key file holds.')
    # parser.add_argument(
    #     '--runner',
    #     dest='runner',
    #     required=True,
    #     default='DataflowRunner',
    #     help='Runner to use')
    parser.add_argument(
        '--project',
        dest='project',
        required=True,
        help='GCP project ID')
    parser.add_argument(
        '--source_bucket_base_path',
        dest='source_bucket_base_path',
        required=True,
        help='GCP source bucket base path')
    parser.add_argument(
        '--source_bucket_name',
        dest='source_bucket_name',
        required=True,
        help='GCP source bucket name')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Add known arguments to pipeline arguments
    pipeline_args += ['--project', known_args.project]

    print('Arguments:')
    print(str(known_args))

    # Then create a service account and the account's authentication key (JSON file)
    # for the service account to authenticate to read/write to a GCP bucket (blob storage)
    # per https://cloud.google.com/docs/authentication/getting-started
    def explicit_auth(key_json_path):
        storage_client = storage.Client.from_service_account_json(key_json_path)
        buckets = list(storage_client.list_buckets())
        return buckets

    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Authenticate to GCP
    # Set environment variable to denote the location of the JSOn holding the authentication key
    # key_json_path = known_args.json_key_path
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = known_args.json_key_path

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Beam pipeline to read all batched JSON files, then parse data into list of tuples
    # Each tuple is a news article (file path, title, news content, hash of news content)

    # Code customised from Google Cloud documentation: https://cloud.google.com/storage/docs/downloading-objects#code-samples
    def download_blob(bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"

        # The ID of your GCS object
        # source_blob_name = "storage-object-name"

        # The path to which the file should be downloaded
        # destination_file_name = "local/path/to/file"

        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        # Construct a client side representation of a blob.
        # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        # any content from Google Cloud Storage. As we don't need additional data,
        # using `Bucket.blob` is preferred here.
        # blob = bucket.blob(source_blob_name)
        blob = bucket.blob(source_blob_name)
        with open(destination_file_name, 'wb') as f:
            # Use TQDM to show progress bar:
            # Ref: https://stackoverflow.com/questions/62811608/how-to-show-progress-bar-when-we-are-downloading-a-file-from-cloud-bucket-using
            # with tqdm.wrapattr(f, 'write', total=blob.size) as file_obj:
            # blob.download_to_filename(destination_file_name)
            storage_client.download_blob_to_file(blob, f)

        print(
            "Downloaded storage object {} from bucket {} to local file {}.".format(
                source_blob_name, bucket_name, destination_file_name
            )
        )

    # Clear any previous YAML files in directory
    for file in [file_nm for file_nm in os.listdir('./') if file_nm.endswith('.yml')]:
        os.remove(file)


    # Open the template YAML config file after copy to local
    download_blob(known_args.source_bucket_name, 'template.yml', 'template.yml')

    with open('template.yml', 'r') as f_read:
        template_config_dct = yaml.safe_load(f_read)

    # Ref: https://beam.apache.org/documentation/io/built-in/google-bigquery/
    table_spec = known_args.project + ':' + known_args.bigquery_output_dataset + '.' + known_args.bigquery_output_table

    table_schema = 'etl_timestamp:DATETIME, body:STRING, url:STRING, label:STRING, title:STRING, file_name:STRING, publication_date:DATETIME, author:STRING, detailed_news_label:STRING, language:STRING, classification_date:DATETIME, country_of_origin:STRING, custom_columns: STRING, body_hash:STRING, url_hash:STRING, title_hash:STRING, file_name_hash:STRING, author_hash:STRING, article_id:STRING'


    # Set authenticate to bigquery with service account JSON
    # Ref: https://cloud.google.com/bigquery/docs/authentication/service-account-file

    key_path = known_args.json_key_path

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=['https://www.googleapis.com/auth/cloud-platform'],
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )


    # Reference: based on https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/tour-of-beam/dataframes.ipynb#scrollTo=g22op8rZPvB3
    with beam.Pipeline(options=pipeline_options) as p:
        records = (p | 'Read copied .csv file' >> beam.io.parquetio.ReadFromParquet(known_args.input)
                   | 'Parse records' >> beam.ParDo(ParseCSV(), bucket_name=known_args.source_bucket_name,
                                                   bucket_base_path=known_args.source_bucket_base_path,
                                                   template_config_dct=template_config_dct))
        (   # Ref: https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/tour-of-beam/dataframes.ipynb#scrollTo=UflW6AJp6-ss
            p | 'PCollection to Pandas Data Frame' >> beam.Create([None])
              | 'As Pandas' >> beam.Map(lambda _, dict_iter: pd.DataFrame(dict_iter), dict_iter=beam.pvalue.AsIter(records),)
              | 'Write to GCP BigQuery' >> beam.ParDo(WriteToBigQuery(), known_args.project,
                                                    known_args.bigquery_output_dataset, known_args.bigquery_output_table)
        )

    result = p.run()
    result.wait_until_finish()

    # Based on https://cloud.google.com/storage/docs/listing-objects#code-samples
    # Based on https://cloud.google.com/storage/docs/listing-objects#code-samples
    def list_blobs(bucket_name):
        """Lists all the blobs in the bucket."""
        # bucket_name = "your-bucket-name"
        storage_client = storage.Client()
        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(bucket_name)
        return [blob.name for blob in blobs]


    # Get the list of files that exist in the import folder
    files_ls = list_blobs(known_args.source_bucket_name)
    pattern = r'^' + known_args.source_bucket_base_path + '\/\w{1,}'
    files_ls = [file_nm for file_nm in files_ls if re.search(pattern, file_nm) is not None]     # Filter for files in the ingestion folder

    # Move the parsed files (data and YAML config file) to the processed folder in GCP cloud storage
    # Ref: https://cloud.google.com/storage/docs/copying-renaming-moving-objects#storage-move-object-python
    def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
        """Moves a blob from one bucket to another with a new name."""
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The ID of your GCS object
        # blob_name = "your-object-name"
        # The ID of the bucket to move the object to
        # destination_bucket_name = "destination-bucket-name"
        # The ID of your new GCS object (optional)
        # destination_blob_name = "destination-object-name"

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
    # Move the processed files to the folder for storing processed items
    for file_nm in files_ls:
        dest_file_nm = file_nm.replace(known_args.source_bucket_base_path + '/', known_args.processed_bucket_success + '/')
        move_blob(known_args.source_bucket_name, file_nm, known_args.source_bucket_name, dest_file_nm)

    # move_blob(known_args.source_bucket_name, 'to_add/*.*', known_args.source_bucket_name, 'added/*.*')

    end_time = datetime.datetime.now()
    print('Completed at: {}'.format(end_time))
    duration = end_time - start_tm
    print('Duration: {}'.format(duration))

    print('END')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
