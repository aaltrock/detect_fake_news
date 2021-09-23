

"""
01 Script to read in the raw .csv file to validate and extract the records into batched JSON files
"""

import pandas as pd
import re
import json
import datetime
import os
from tqdm import tqdm
import logging
import os
from google.cloud import storage


# Then create a service account and the account's authentication key (JSON file)
# for the service account to authenticate to read/write to a GCP bucket (blob storage)
# per https://cloud.google.com/docs/authentication/getting-started
def explicit_auth(key_json_path):
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

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )


# Based on https://cloud.google.com/storage/docs/listing-objects#code-samples
def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    return blobs


# Main execution
def run(src_path, res_path, batch_sz):
    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Name of bucket to read and write
    read_bucket_nm = 'fake_news_raw'
    write_bucket_nm = 'fake_news_cleaned_json'

    # Authenticate to GCP
    # Set environment variable to denote the location of the JSOn holding the authentication key
    key_json_path = 'detect-fake-news-313201-3e1c9965ac91.key.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_json_path
    buckets = explicit_auth(key_json_path)

    # List the buckets available after explicit declaration
    print('\nList of GCP buckets accessible by service account:')
    for bucket in buckets:
        print(bucket)

    # Get the list of files available in storage
    res = list_blobs(read_bucket_nm)
    print(type(res))
    print('\nFiles in {}:'.format(read_bucket_nm))
    read_bucket_file_ls = [blob.name for blob in res]
    print(read_bucket_file_ls)

    # Get the raw file name (1st element since there is only one file)
    src_file_nm = read_bucket_file_ls[0]

    # Download raw file from GCP bucket
    print('Downloading from bucket {} the file {}...'.format(read_bucket_nm, src_file_nm))
    download_blob(read_bucket_nm, src_file_nm, src_file_nm)

    print('Readline file {}...'.format(src_path))

    # Read in whole CSV files as lines
    file1 = open(src_path, 'r')
    Lines = file1.readlines()

    def find_record_beginnings(Lines):
        beginning_lines_nr_ls = []
        for i, line in enumerate(Lines):
            # If line begins n,n,*,*,*,"* pattern then new record begins (Index, News ID, URL, Label, "News article)
            new_record_check = re.match(r'^\d+,\d{1,},\w{1,}', line) is not None
            if new_record_check:
                beginning_lines_nr_ls += [i]
        print('No. of line beginnings found: {}'.format(len(beginning_lines_nr_ls)))
        return beginning_lines_nr_ls

    beginning_lines_nr_ls = find_record_beginnings(Lines)

    def loop_lines(Lines, beginning_lines_nr_ls, res_path, batch_sz):

        def parse_str_to_dict(str):
            res_dct = {}

            # First split by double quotes (which wrap the free text)
            split_by_quote_ls = str.split('"')

            # If parsed elements do not have min three elements split by '"' then return None
            if len(split_by_quote_ls) < 3:
                return None

            """
            Process 1st part of string - index, news ID, domain, label and URL fields
            """
            # First part split by ',' to get individual field values
            first_part_ls = split_by_quote_ls[0].split(',')
            # Discard last part contain '"'
            first_part_ls = first_part_ls[:-1]
            # If retained list elements not 5, corrupt data then discard
            if len(first_part_ls) != 5:
                return None
            else:
                res_dct.update({'idx': first_part_ls[0],
                                'news_id': first_part_ls[1],
                                'domain': first_part_ls[2],
                                'label': first_part_ls[3],
                                'url': first_part_ls[4]
                                })

            """
            Process second part of string - news article free text
            """
            # Find which split part contains time stamp
            def __has_time_stamp(s):
                return re.search(r'^,\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.\d+', s) is not None or \
                       re.search(r'^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.\d+', s) is not None

            # Get index of list split by '"' with time stamp
            timestamp_index = None
            for i, elem in enumerate(split_by_quote_ls):
                if __has_time_stamp(elem):
                    timestamp_index = i
                    break

            # If time stamp element found, get second elements to before this element as text body string
            if timestamp_index is not None:
                # If time stamp found is after 2nd element, pack news article string to dict otherwise return None
                if timestamp_index >= 1:
                    # Between 2nd element in split string by '"' to before timestamp index are the free text
                    news_article = ''.join(split_by_quote_ls[1:timestamp_index])
                    res_dct.update({'body': news_article})
                else:
                    return None
            else:
                return None

            """
            Process timestamps
            """
            time_stamp_ls = split_by_quote_ls[timestamp_index].split(',')
            time_stamp_ls = [timestamp for timestamp in time_stamp_ls
                             if re.search(r'\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.\d+', timestamp) is not None]
            # If three time stamps found, pack to parsed dictionary, otherwise return None
            if len(time_stamp_ls) == 3:
                res_dct.update({'scraped_at': time_stamp_ls[0],
                                'inserted_at': time_stamp_ls[1],
                                'updated_at': time_stamp_ls[2]
                                })
            else:
                return None

            """
            Process third part of string - title after time stamp
            """
            # If there is element after time stamp element, then get title and pack to dict, otherwise return None
            if timestamp_index + 1 <= len(split_by_quote_ls) - 1:
                title = split_by_quote_ls[timestamp_index + 1]
                # Check if title is a free text string and not an array (which indicates not a title and parsed wrongly)
                if not(re.search(r'^\[', title) is not None and re.search(r']$', title) is not None):
                    res_dct.update({'title': title})
                else:
                    return None

                # Check if title is short return None, as it indicates parsing wrong string
                if len(title) <= 10:
                    return None

                # Check if digit characters make up over 50% of the title string
                if sum(c.isdigit() for c in title)/len(title) > .5:
                    return None

                # Check if title is likely to be a list of authors' names by average length of 3 words per phrase between ","
                # E.g. 'Happy Cog Studios - Http, Www.Happycog.Com, Daily Kos, Joan Mccarter, Jesse Sbaih For Us Senate,
                # Bob Johnson, Leslie Salzillo, Mark Sumner'
                word_len_ls = [len(phrase.split(' ')) for phrase in title.split(',')]
                if sum(word_len_ls) / len(word_len_ls) <= 3:
                    return None

            else:
                return None

            # For the rest of the elements - authors etc, because they were not reliable, they are discarded

            return res_dct

        # If enough parsed records exist to batch size, write to file
        def write_json(parsed_dct, batch_nr, res_path, dest_bucket_nm):
            # Write to local
            dest_file_nm = os.path.join(res_path, str(batch_nr) + '.json')
            with open(dest_file_nm, 'w') as f:
                __json_str = json.dumps(parsed_dct, indent=4) + '\n'
                print(__json_str, file=f)

            # Write to GCP
            gcp_blob_nm = str(batch_nr) + '.json'
            upload_blob(dest_bucket_nm, dest_file_nm, gcp_blob_nm)

        batch_nr = 1
        parsed_dct = {}
        __parsed_string = ''
        parsed_record_count = 0

        next_lines_nr_ls = beginning_lines_nr_ls[1:]
        # Create a tuple of current and upper bound lines (exclusive) to extract full strings
        curr_next_lines_tup_ls = [(curr_line, next_line) for curr_line, next_line in
                                  zip(beginning_lines_nr_ls[:-1], next_lines_nr_ls)]

        for i, (curr_line_nr, ubound_line_nr) in tqdm(enumerate(curr_next_lines_tup_ls)):
            # print('Parsing between lines {} and {}...'.format(curr_line_nr, ubound_line_nr), end='\r')
            __parsed_string = ''.join(Lines[curr_line_nr: ubound_line_nr])

            # Break string into different elements (return None if invalid string)
            record_json = parse_str_to_dict(__parsed_string)

            # If valid parsed dict then pack to results dictionary
            if record_json is not None:
                parsed_record_count += 1

                # Pack parsed dictionary to final results dictionary
                parsed_dct.update({record_json.get('news_id'): record_json.copy()})

            if len(parsed_dct) >= batch_sz:
                write_json(parsed_dct, batch_nr, res_path, write_bucket_nm)

                # Reset parsed dict and increment batch counter
                parsed_dct = {}
                batch_nr += 1

        # Write any remaining parsed records to JSON file
        if len(parsed_dct) >= 0:
            write_json(parsed_dct, batch_nr, res_path)

        print('No. of records parsed successfully: {}'.format(parsed_record_count))
        end_time = datetime.datetime.now()
        print('Completed at: {}'.format(end_time))
        duration = end_time - start_tm
        print('Duration: {}'.format(duration))

    loop_lines(Lines, beginning_lines_nr_ls, res_path, batch_sz)


src_path = r'/home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow/Copy of news_cleaned_2018_02_13.csv'
# src_path = r'/home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow/news_sample.csv'
res_path = r'/home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow/json_src'
# batch_sz = 500
batch_sz = 1
run(src_path, res_path, batch_sz)

print('END')
