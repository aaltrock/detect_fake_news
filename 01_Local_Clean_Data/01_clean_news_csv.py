"""
# STEP 1 SCRIPT - READ IN RAW NEWS ARTICLE CSV FILE
# Repair the inconsistent quoting (which only applied to the news articles column) that leads to delimiter breaking
# Save into a new CSV file with | separator and full quoting (of all columns) for step 2 to parse into individual JSONs
"""


if __name__ == '__main__':
    import pandas as pd
    import re
    import json
    import sys
    import time
    import csv
    parsed_ls = []

    # Read CSV file with quoting then save into a new CSV with full quoting for Step 2 script
    src_df = pd.read_csv('news_cleaned_2018_02_13.csv', sep=',', encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    src_df.rename({'Unnamed: 0': 'ID'}, inplace=True)
    src_df.to_csv('news_cleaned_2018_02_13_full_quote.csv', sep='|', index=False, quoting=csv.QUOTE_ALL)
    print('END')
