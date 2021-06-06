"""
# STEP 2 SCRIPT - PARSE PANDAS DATA FRAME OF NEWS ARTICLES INTO INDIVIDUAL JSON FILES & A NEW CSV FILE
# Step 2 - ETL: parse Pandas data frame 'content' and 'title' news article columns into separate dictionaries
# Step 2B - The remaining data frame columns (ie meta data) are written to CSV file for further cleansing.
"""


if __name__ == '__main__':
    import pandas as pd
    import json
    import csv
    import tqdm
    import pickle
    import os

    parsed_ls = []
    dct_folder_nm = 'parse_dict'
    dest_path = os.path.join(os.getcwd(), dct_folder_nm)

    tqdm.tqdm.pandas()
    print('Reading in {}...'.format('news_cleaned_2018_02_13_full_quote.csv'))
    src_df = pd.read_csv('news_cleaned_2018_02_13_full_quote.csv', sep='|',
                         encoding='utf-8', quoting=csv.QUOTE_ALL, header=0)
    print('Dimension: {} by {}'.format(src_df.shape[0], src_df.shape[1]))

    # Dump rows without valid classification
    print('Excluding rows without valid news article type values...')
    valid_class_ls = ['rumor', 'hate', 'unreliable', 'conspiracy', 'clickbait', 'satire', 'fake', 'reliable',
                      'bias', 'political', 'junksci', 'unknown']
    src_df['is_valid_type'] = src_df['type'].progress_apply(lambda val: val in valid_class_ls)
    cleaned_df = src_df[src_df['is_valid_type']].copy()
    print('After dropping rows without valid classifications: Dimension: {} by {}'
          .format(cleaned_df.shape[0], cleaned_df.shape[1]))
    rows_dropped = src_df.shape[0] - cleaned_df.shape[0]
    print('No. of records dropped: {}'.format(rows_dropped))

    # Create a binary classification column to denote fake news or not
    print('Creating binary classifications...')
    cleaned_df['is_unreliable'] = cleaned_df['type'].map(lambda val: True if val != 'reliable' else False)
    print('Of which no. of unreliable items: {}'.format(cleaned_df[cleaned_df['is_unreliable']].shape[0]))

    # Create JSON file name by row number
    print('Creating JSON file names...')
    cleaned_df['row_index'] = list(range(cleaned_df.shape[0]))
    cleaned_df['json_file_nm'] = cleaned_df['row_index'].progress_apply(lambda val: str(val) + '.json')
    cleaned_df.drop(columns=['Unnamed: 0'], inplace=True)

    # Drop the title and content columns of cleaned_df then write as csv file
    cleaned_df.to_csv('cleaned_df.csv', index=False)

    print('END')
