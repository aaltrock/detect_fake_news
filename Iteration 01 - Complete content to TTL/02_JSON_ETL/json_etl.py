import argparse
import logging
import re
import os
import glob
import json
import datetime
import hashlib

import rdflib
from rdflib.namespace import Namespace
from rdflib.namespace import OWL, RDF, RDFS, XSD
from rdflib import URIRef, BNode, Literal
import urllib

import apache_beam as beam
from apache_beam.io import ReadFromText, ReadAllFromText, fileio
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# Reference: the pipeline was written based on the tutorial set out in as a template: https://beam.apache.org/get-started/wordcount-example/

# DoFn class to write Turtle strings into TTL text files
class WriteTurtles(beam.DoFn):
    def process(self, element):
        input_path = element[0]
        ttl_str = element[1]
        output_path = element[2]

        writer = fileio.filesystems.FileSystems.create(output_path)
        writer.write(ttl_str)
        writer.close()


# DoFn class to create a RDFLib graph with name space, classesand objects to populate entities later
class MakeRDFLibGraph(beam.DoFn):

    def process(self, element):
        file_path = element[0]
        out_path = element[1]

        # Instantiate RDFLib graph object to add triples
        g = rdflib.Graph()

        # Prefix and bind to graph
        aa = Namespace('http://www.city.ac.uk/ds/inm363/aaron_altrock#')
        g.bind('aa', aa)

        """
        Classes Triples
        """
        # Define classes used in triples
        classes_ls = [aa.newsId, aa.title, aa.content, aa.contentHash]

        for cls in classes_ls:
            g.add((cls, RDF.type, OWL.Class))

        """
        Object Triples - Pack individual triples from JSON files content
        """
        object_ls = [aa.has_title, aa.has_content, aa.has_content_hash]
        for obj in object_ls:
            g.add((obj, RDF.type, OWL.ObjectProperty))

        yield file_path, g, out_path


# DoFn class to serialise RDFLib graph after filling in with entities into Turtle strings
class SerializeRDFLibGraph(beam.DoFn):

    @staticmethod
    def __read_json(file_path):
        print('File: {}'.format(file_path))
        with open(file_path) as read_file:
            json_obj = json.load(read_file)
        # print(json_obj)
        return json_obj

    def process(self, element):
        file_path = element[0]
        g = element[1]
        out_path = element[2]

        # # Prefix and bind to graph
        aa = Namespace('http://www.city.ac.uk/ds/inm363/aaron_altrock#')

        """
        Add Individuals
        """

        print('File: {}'.format(file_path))
        with open(file_path) as read_file:
            newsId_newsContent_dct = json.load(read_file)

        # Exclude where news content is completely None
        def __parse_dct(_news_id, content_dct):

            # Coalesce to default 'null' if content or news title do not exist
            if content_dct.get('title') is not None:
                _title = str(content_dct.get('title'))
                _title_hash = hashlib.md5(_title.encode('utf-8')).hexdigest()
            else:
                _title = None
                _title_hash = None

            if content_dct.get('content') is not None:
                _content = str(content_dct.get('content'))
                _content_hash = hashlib.md5(_title.encode('utf-8')).hexdigest()
            else:
                _content = None

            return _news_id, _title, _content, _title_hash, _content_hash

        # res_dct = dict()
        # res_ls = []
        if newsId_newsContent_dct is not None:
            for news_id, news_dct in newsId_newsContent_dct.items():
                if news_dct is not None:
                    news_id, title, content, title_hash, content_hash = __parse_dct(news_id, news_dct)
                    # res_dct.update({str([news_id, 'title']): (news_id, 'has_title', title)})
                    # res_dct.update({str([news_id, 'content']): (news_id, 'has_content', content)})
                    # res_dct.update({str([news_id, 'content_hash']): (news_id, 'has_content_hash', content_hash)})

                    # Make URI and literals for subjects and objects
                    news_id_uri = aa[urllib.parse.quote(str(news_id))]
                    news_id_lit = Literal(news_id)

                    # Add triples - literals
                    g.add((news_id_uri, aa.label, news_id_lit))

                    if title is not None and title_hash is not None:
                        title_uri = aa[urllib.parse.quote(str(title_hash))]
                        title_lit = Literal(title)
                        g.add((title_uri, aa.label, title_lit))
                    else:
                        title_uri = rdflib.BNode

                    if content is not None and content_hash is not None:
                        # content_uri = aa[urllib.parse.quote(str(content))]
                        content_lit = Literal(content)
                        content_hash_uri = aa[urllib.parse.quote(str(content_hash))]
                        g.add((content_hash_uri, aa.label, content_lit))
                    else:
                        content_hash_uri = rdflib.BNode

                    # g.add((content_uri, aa.label, content_lit))
                    # g.add((content_hash_uri, aa.label, content_hash_lit))

                    # Add triples - subject, predicate, object
                    g.add((news_id_uri, aa.has_title, title_uri))
                    # g.add((news_id_uri, aa.has_content, content_uri))
                    # g.add((news_id_uri, aa.has_content_hash, content_hash_uri))
                    g.add((news_id_uri, aa.has_content, content_hash_uri))

        # yield res_ls

        # res = {news_id: __parse_dct(news_id, news_dct) for news_id, news_dct in newsId_newsContent_dct.items() if news_dct is not None]
        # return res
        # yield res_dct
        ttl_str = g.serialize(destination=None, format='ttl')
        # yield res_dct
        # yield g
        yield file_path, ttl_str, out_path


def run(argv=None, save_main_session=True):
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

    # Create a tuple file listing (input path, output path)
    file_ls = [(input_file_path, os.path.join(known_args.output, os.path.basename(input_file_path)) + '.ttl') for input_file_path in file_ls]

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
                          # | 'Tuple of file path and RDFLib Graph' >> beam.Map(lambda x: (x, make_graph()))
                          | 'Shuffle' >> beam.transforms.util.Reshuffle()
                          | 'Make RDFLib graph' >> beam.ParDo(MakeRDFLibGraph())
                          | 'Serialise RDFLib graph' >> beam.ParDo(SerializeRDFLibGraph())
                          | 'Parallel write Turtles' >> beam.ParDo(WriteTurtles())
                          # | 'Serialize RDFLib graph' >> beam.ParDo(RDFLibGraphSerialize())
                          # | 'Print' >> beam.Map(lambda x: print('File: {}'.format(x)))
                          # | 'Combine RDFLib graph' >> beam.CombineGlobally(sum)
                          # | 'Serialize Graph' >> beam.Map(lambda g: g.serialize(destination=None, format='ttl'))
                          # | 'Write' >> beam.io.Write(beam.io.WriteToText(known_args.output))
                          # | 'Count all elements' >> beam.combiners.Count.Globally()
                          # | 'print' >> beam.Map(print)
                          # | 'To dict' >> beam.transforms.combiners.ToList()
                          )
        # readable_files | 'Write' >> WriteToText(known_args.output)
        # readable_files | 'Write' >> beam.io.Write(beam.io.WriteToText(known_args.output))
        # print('No. of files read:')
        # read_files_count = (readable_files | 'Count all elements' >> beam.combiners.Count.Globally()
        #                     | 'print' >> beam.Map(print))

        # readable_files | 'Get output JSON size' >> beam.Map(
        #     lambda dct: print('No. of elements in output JSON: {}'.format(len(dct))))

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
