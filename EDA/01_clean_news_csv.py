# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import pandas as pd
    import re
    import json
    import sys
    import time
    import csv
    parsed_ls = []

    src_df = pd.read_csv('news_cleaned_2018_02_13.csv', sep=',', encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    src_df.rename({'Unnamed: 0': 'ID'}, inplace=True)
    src_df.to_csv('news_cleaned_2018_02_13_full_quote.csv', index=False, quoting=csv.QUOTE_ALL)
    print('END')
