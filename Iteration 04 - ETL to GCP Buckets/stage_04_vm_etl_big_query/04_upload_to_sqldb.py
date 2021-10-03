
import re
import json
import datetime
from tqdm import tqdm
import os
from google.cloud import storage, bigquery
import pandas as pd


# Then create a service account and the account's authentication key (JSON file)
# for the service account to authenticate to read/write to a GCP bucket (blob storage)
# per https://cloud.google.com/docs/authentication/getting-started
def bucket_explicit_auth(key_json_path):
    storage_client = storage.Client.from_service_account_json(key_json_path)
    buckets = list(storage_client.list_buckets())
    return buckets


# Pre-requisite: need to grant service account the permission Storage Admin role to the bucket
# per https://cloud.google.com/storage/docs/uploading-objects

# Code customised from Google Cloud documentation: https://cloud.google.com/storage/docs/uploading-objects
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


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
        with tqdm.wrapattr(f, 'write', total=blob.size) as file_obj:
            # blob.download_to_filename(destination_file_name)
            storage_client.download_blob_to_file(blob, file_obj)

    # print(
    #     "Downloaded storage object {} from bucket {} to local file {}.".format(
    #         source_blob_name, bucket_name, destination_file_name
    #     )
    # )


# Based on https://cloud.google.com/storage/docs/listing-objects#code-samples
def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    return blobs


# Read a JSON file
def read_json(file_path):
    # print('File: {}'.format(file_path))
    with open(file_path) as read_file:
        json_obj = json.load(read_file)
    # print(json_obj)
    return json_obj


# Main execution
def run():
    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Name of bucket to read
    read_bucket_nm = 'fake_news_ttl_json'

    # Name of BigQuery data set, schema, and table name to write
    project_id = 'detect-fake-news-313201'
    write_dataset_nm = 'fake_news_sql'
    write_tbl_nm = write_dataset_nm + '.' + 'src_fake_news'

    # Authenticate to GCP
    # Set environment variable to denote the location of the JSOn holding the authentication key
    key_json_path = 'detect-fake-news-313201-3e1c9965ac91.key.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_json_path
    buckets = bucket_explicit_auth(key_json_path)

    # List the buckets available after explicit declaration
    print('\nList of GCP buckets accessible by service account:')
    for bucket in buckets:
        print(bucket)

    # Get the list of files available in storage
    res = list_blobs(read_bucket_nm)
    print(type(res))
    print('\nFiles in {}:'.format(read_bucket_nm))

    # Filter for the list of files in JSON format
    read_bucket_file_ls = [blob.name for blob in res if re.search('.json$', blob.name) is not None]
    print(read_bucket_file_ls)

    # Final data frame
    final_df_ls = []

    # Loop through each file
    print('Reading through each JSON file to parse into data frame...')
    for file_nm in tqdm(read_bucket_file_ls):
        # Download the file to local from GCP
        download_blob(read_bucket_nm, file_nm, file_nm)

        # Read in JSON file
        src_json = read_json(file_nm)

        # Repack the dictionary to be all at one level
        url_hash = list(src_json.items())[0][0]
        content_dct = list(src_json.items())[0][1]
        content_dct.update({'url_hash': url_hash})

        # Turn into a dataframe
        col_nm_ls = [k for k, v in list(content_dct.items())]
        __df = pd.DataFrame.from_dict(content_dct.copy(), orient='index').T

        # Append data frame to list of data frames
        final_df_ls += [__df]

        # Delete the file from local directory
        os.remove(file_nm)

    # Parse list of data frames into the final dataframe
    print('Concatenating {} data frames into the final one...'.format(len(final_df_ls)))
    fin_df = pd.concat(final_df_ls, axis=0)
    print('Final dataframe dimension: {} x {}'.format(fin_df.shape[0], fin_df.shape[1]))

    # Write to BigQuery (overwrite)
    print('Writing final dataframe to GCP Big Query table {}...'.format(write_tbl_nm))
    fin_df.to_gbq(destination_table=write_tbl_nm, project_id=project_id, if_exists='replace')

    # Get duration taken to process
    end_time = datetime.datetime.now()
    print('Completed at: {}'.format(end_time))
    duration = end_time - start_tm
    print('Duration: {}'.format(duration))


if __name__ == '__main__':
    run()

    print('END')

