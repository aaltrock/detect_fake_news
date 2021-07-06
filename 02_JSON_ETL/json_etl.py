#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# pytype: skip-file

import argparse
import logging
import re
import os
import glob
import json
import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText, ReadAllFromText, fileio
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# Flatten nested JSON items in each batch JSON file
class FlattenDictFn(beam.DoFn):

    @staticmethod
    def __read_json(file_path):
        print('File: {}'.format(file_path))
        with open(file_path) as read_file:
            json_obj = json.load(read_file)
        # print(json_obj)
        return json_obj

    def process(self, element):
        file_path = element

        # dct = self.__read_json(file_path).copy()

        print('File: {}'.format(file_path))
        with open(file_path) as read_file:
            newsId_newsContent_dct = json.load(read_file)

        # Exclude where news content is completely None
        def __parse_dct(news_id, content_dct):

            # Coalesce to default 'null' if content or news title do not exist
            if content_dct.get('title') is not None:
                title = content_dct.get('title')
            else:
                title = 'null'

            if content_dct.get('content') is not None:
                content = content_dct.get('content')
            else:
                content = 'null'

            return news_id, title, content, hash(content)

        res_dct = dict()
        # res_ls = []
        if newsId_newsContent_dct is not None:
            for news_id, news_dct in newsId_newsContent_dct.items():
                if news_dct is not None:
                    news_id, title, content, content_hash = __parse_dct(news_id, news_dct)
                    res_dct.update({str([news_id, 'title']): title})
                    res_dct.update({str([news_id, 'content']): content})
                    res_dct.update({str([news_id, 'content_hash']): content_hash})
                    # if content != 'null':
                    #     res_ls += [(str((news_id, 'title')), title),
                    #                (str((news_id, 'content')), content),
                    #                (str((news_id, 'content_hash')), content_hash)]

        # yield res_ls

        # res = {news_id: __parse_dct(news_id, news_dct) for news_id, news_dct in newsId_newsContent_dct.items() if news_dct is not None]
        # return res
        yield res_dct


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      default=os.path.join(os.getcwd(), 'parse_dict'),
      help='Batched JSON files.')
    parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    json_file_list = glob.glob(known_args.input + '/**/*.json', recursive=True)
    print('No. of JSON (batched) files found: {}'.format(len(json_file_list)))

    file_path = known_args.input + '/*.*'
    print('File path: {}'.format(file_path))
    file_ls = glob.glob(file_path)

    def read_json(file_path):
        print('File: {}'.format(file_path))
        with open(file_path) as read_file:
            json_obj = json.load(read_file)
        # print(json_obj)
        return json_obj

    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Beam pipeline to read all batched JSON files, then parse data into list of tuples
    # Each tuple is a news article (file path, title, news content, hash of news content)
    with beam.Pipeline(options=pipeline_options) as p:
        readable_files = (p
                          # | 'Get list of JSON files' >> fileio.MatchFiles(file_path)
                          # | 'Find files' >> fileio.ReadMatches()
                          | 'Create PCollection' >> beam.Create(file_ls)
                          # | 'List file' >> beam.Map(lambda x: print(x))
                          # | 'Read file' >> beam.Map(lambda x: (x.metadata.path, read_json(x.metadata.path)))
                          # | 'Read file' >> beam.Map(lambda x: (x, read_json(x)))
                          | 'Shuffle' >> beam.transforms.util.Reshuffle()
                          | 'Parse JSON files' >> beam.ParDo(FlattenDictFn())
                          # | 'Print' >> beam.Map(lambda x: print('File: {}'.format(x)))
                          | 'Write' >> beam.io.Write(beam.io.WriteToText(known_args.output))
                          # | 'Count all elements' >> beam.combiners.Count.Globally()
                          # | 'print' >> beam.Map(print)
                          | 'To dict' >> beam.transforms.combiners.ToList()
                          )
        # readable_files | 'Write' >> WriteToText(known_args.output)
        # readable_files | 'Write' >> beam.io.Write(beam.io.WriteToText(known_args.output))
        # print('No. of files read:')
        # read_files_count = (readable_files | 'Count all elements' >> beam.combiners.Count.Globally()
        #                     | 'print' >> beam.Map(print))

        readable_files | 'Get output JSON size' >> beam.Map(lambda dct: print('No. of elements in output JSON: {}'.format(len(dct))))

        # res_ls = (readable_files
        #           | 'To list' >> beam.combiners.ToList()
        #           # | 'Flatten list' >> beam.Map(lambda ls: [x for y in ls for x in y])
        #           # | 'Count list' >> beam.Map(lambda ls: print('No. of news articles parsed: {}'.format(len(ls))))
        #           # | 'Write to file' >> beam.io.Write(beam.io.WriteToText(known_args.output))
        #           )

        # res_ls | 'Write to file' >> beam.io.Write(beam.io.WriteToText(known_args.output))
        #
        # res_ls | 'Count list' >> beam.Map(lambda ls: print('No. of news articles parsed: {}'.format(len(ls))))

        # print('type(res_ls): {}'.format(str(type(res_ls))))

    result = p.run()
    result.wait_until_finish()

    end_tm = datetime.datetime.now()
    print('Finished at: {}'.format(end_tm))
    print('Output file path: {}'.format(known_args.output))

    tm_taken = end_tm - start_tm
    print('Duration: {}'.format(tm_taken))

    print('END')

    # counts = (
    #     lines
    #     | 'Split' >> (beam.ParDo(HashDoFn()).with_output_types(str))
    #     | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
    #     | 'GroupAndSum' >> beam.CombinePerKey(sum))
    #
    # # Format the counts into a PCollection of strings.
    # def format_result(word, count):
    #   return '%s: %d' % (word, count)
    #
    # output = counts | 'Format' >> beam.MapTuple(format_result)
    #
    # # Write the output using a "Write" transform that has side effects.
    # # pylint: disable=expression-not-assigned
    # output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
