"""
# STEP 3 SCRIPT - PARSE PANDAS DATA FRAME INTO JSON & META DATA TO SEPARATE CSV FILE
# Note: after that run locally in Terminal to push the output data to GCP Bucket storage:
# 1. Install Google Cloud SDK
# Run the following in multiple terminals to upload concurrently:
# 2. 'gcloud init' to link local terminal to GCP project and authenticate
# 3. 'gsutil cp -r parse_dct gs://src_fake_news' to copy files to bucket
# 4. 'gsutil cp cleaned_df.csv gs://src_fake_news' to copy files to bucket
# 5. 'gsutil cp cleaned_meta_df.csv  gs://src_fake_news' to copy files to bucket
"""


if __name__ == '__main__':
    import pandas as pd
    import json
    import csv
    import tqdm
    import os

    parsed_ls = []
    dct_folder_nm = 'parse_dict'

    # Set destination path and make directory
    dest_path = os.path.join(os.getcwd(), dct_folder_nm)
    os.makedirs(dest_path, exist_ok=True)
    
    # No. of news articles to pack into JSON file
    batch_sz = 50000

    tqdm.tqdm.pandas()
    print('Reading in {}...'.format('cleaned_df.csv'))
    cleaned_df = pd.read_csv('cleaned_df.csv', encoding='utf-8', quoting=csv.QUOTE_ALL, header=0)
    print('Dimension: {} by {}'.format(cleaned_df.shape[0], cleaned_df.shape[1]))

    # Turn each row into its own dictionary, then pack all dicts to a list
    print('Parsing source data frame into a dictionary...')

    json_content_dct = {}
    j = 1
    for i, row in tqdm.tqdm(cleaned_df.iterrows()):
        idx = row['row_index']

        if i % batch_sz == 0 and i != 0:
            __json_file_nm = os.path.join(dest_path, 'content_dct_part_' + str(j) + '.json')
            with open(__json_file_nm, 'w') as f:
                __json_str = json.dumps(json_content_dct, indent=4) + '\n'
                print(__json_str, file=f)

            # Reset dictionary
            json_content_dct = {}
            j += 1

        # __dct = pd.DataFrame(row['Content']).to_dict(orient='index')
        json_content_dct.update({idx: {'title': row['title'],
                                       'content': row['content']}
                                 })

    # Drop the title and content columns of cleaned_df then write as csv file
    cleaned_df.drop(columns=['content', 'title'], inplace=True)
    cleaned_df.to_csv('cleaned_meta_df.csv', index=False)

    print('END')
