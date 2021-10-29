"""
01 Script to read in the raw .csv file to validate and extract the records into batched JSON files
"""

import pandas as pd
import re
import json
import datetime
import os
from tqdm import tqdm

def run(src_path, res_path, batch_sz):
    print('Readline file {}...'.format(src_path))

    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

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

            # If prased elements does not have min three elements split by '"' then return None
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
        def write_json(parsed_dct, batch_nr, res_path):
            dest_file_nm = os.path.join(res_path, str(batch_nr) + '.json')
            with open(dest_file_nm, 'w') as f:
                __json_str = json.dumps(parsed_dct, indent=4) + '\n'
                print(__json_str, file=f)

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
                write_json(parsed_dct, batch_nr, res_path)

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


src_path = r'/Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow/raw/news_cleaned_2018_02_13.csv'
# src_path = r'/Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow/raw/news_sample.csv'
res_path = r'/Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow/json_src'
batch_sz = 500
run(src_path, res_path, batch_sz)

print('END')