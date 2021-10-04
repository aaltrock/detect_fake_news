"""
02 Script to read in the batched JSON files and parse them into TTL files
"""

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


# DoFn class to write Turtle strings into TTL text files
class WriteTurtleJSON(beam.DoFn):
    def process(self, element):
        input_path = element[0]
        ttl_str = element[1]
        url, domain, body_hash, body = element[2]
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


# DoFn class to create a RDFLib graph with name space, classesand objects to populate entities later
class MakeRDFLibGraph(beam.DoFn):

    def process(self, element):
        file_path = element[0]
        out_path_ttl = element[1]
        out_path_json = element[2]

        # Instantiate RDFLib graph object to add triples
        g = rdflib.Graph()

        # Prefix and bind to graph
        aa = Namespace('http://www.city.ac.uk/ds/inm363/aaron_altrock#')
        g.bind('aa', aa)

        """
        Classes Triples
        """
        # Define classes used in triples
        classes_ls = [aa.newsId, aa.title, aa.domain, aa.url, aa.newsLabel,
                      aa.bodyHash]

        for cls in classes_ls:
            g.add((cls, RDF.type, OWL.Class))

        """
        Object Triples - Pack individual triples from JSON files content
        """
        object_ls = [aa.has_id, aa.has_url, aa.has_title, aa.has_body_hash, aa.has_news_label,
                     aa.scraped_at, aa.inserted_at, aa.updated_at]
        for obj in object_ls:
            g.add((obj, RDF.type, OWL.ObjectProperty))

        """
        Data Properties
        """
        data_prop_ls = [aa.scrapedDate, aa.insertedDate, aa.updatedDate]
        for data_prop in data_prop_ls:
            g.add((data_prop, RDF.type, OWL.DataProperty))

        yield file_path, aa, g, out_path_ttl, out_path_json


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
        aa = element[1]
        g = element[2]
        out_path_ttl = element[3]
        out_path_json = element[4]

        """
        Add Individuals
        """

        print('File: {}'.format(file_path))
        with open(file_path) as read_file:
            newsId_newsContent_dct = json.load(read_file)

        # Exclude where news content is completely None
        def __parse_dct(_news_id, content_dct):

            # If any of the URL, domain, label, body is blank, return None (i.e. discard sample)
            if content_dct.get('url') is None or content_dct.get('domain') is None or \
                    content_dct.get('label') is None or content_dct.get('body') is None:
                return None
            else:
                _url = content_dct.get('url')
                _domain = content_dct.get('domain')
                _label = content_dct.get('label')
                _body = str(content_dct.get('body'))
                _body_hash = 'body_' + hashlib.md5(_body.encode('utf-8')).hexdigest()

            def __coalesce_val(dct, key):
                if dct.get(key) is not None:
                    return dct.get(key)
                else:
                    return None

            # Coalesce to default None if content or news title do not exist
            # for title, scraped_at, inserted_at, updated_at
            _title = __coalesce_val(content_dct, 'title')
            _scraped_at = __coalesce_val(content_dct, 'scraped_at')
            _inserted_at = __coalesce_val(content_dct, 'inserted_at')
            _updated_at = __coalesce_val(content_dct, 'updated_at')

            return _news_id, _domain, _url, _label, _title, _body, _body_hash, _scraped_at, _inserted_at, _updated_at

        news_body_dct = {}  # Dict to pack with news body if validation passes below
        if newsId_newsContent_dct is not None:
            for news_id, news_dct in newsId_newsContent_dct.items():
                if news_dct is not None:
                    # Parse each sample dictionary from JSON into the individual elements
                    parse_res = __parse_dct(news_id, news_dct)

                    # If parsed results is not None (i.e. key elements exist), add as triples into RDF graph
                    if parse_res is not None:
                        (news_id, domain, url, label, title, body, body_hash,
                         scraped_at, inserted_at, updated_at) = parse_res

                        # print('Parsing sample with URL: {}'.format(url))

                        # g.add((news_id, RDF.type, aa.newsId))
                        # g.add((url, RDF.type, aa.url))
                        # g.add((label, RDF.type, aa.newsLabel))
                        # g.add((title, RDF.type, aa.title))
                        # g.add((body_hash, RDF.type, aa.bodyHash))

                        # Make URI and literals for subjects and objects
                        url_uri = aa[urllib.parse.quote(str(url))]
                        url_lit = Literal(url)
                        g.add((url_uri, RDF.type, aa.url))
                        g.add((url_uri, aa.has_url, url_lit))

                        news_id_uri = aa[urllib.parse.quote(str(news_id))]
                        news_id_lit = Literal(news_id)
                        g.add((news_id_uri, RDF.type, aa.newsId))
                        g.add((news_id_uri, RDFS.label, news_id_lit))
                        g.add((url_uri, aa.has_id, news_id_uri))

                        label_uri = aa[urllib.parse.quote(label)]
                        label_lit = Literal(label)
                        g.add((label_uri, RDF.type, aa.newsLabel))
                        g.add((label_uri, RDFS.label, label_lit))
                        g.add((url_uri, aa.has_news_label, label_lit))

                        domain_uri = aa[urllib.parse.quote(str(domain))]
                        domain_lit = Literal(domain)
                        g.add((domain_uri, RDF.type, aa.domain))
                        g.add((domain_uri, RDFS.label, domain_lit))
                        g.add((url_uri, aa.has_domain, domain_uri))

                        # Use the hash of the title as the URI
                        if title is not None:
                            title_hash = 'title_' + hashlib.md5(title.encode('utf-8')).hexdigest()
                            title_uri = aa[urllib.parse.quote(title_hash)]
                            title_lit = Literal(title)
                            g.add((title_uri, RDF.type, aa.title))
                            g.add((title_uri, RDFS.label, title_lit))
                            g.add((url_uri, aa.has_title, title_uri))
                        else:
                            # Add BNode if sample has no title
                            g.add((url_uri, aa.has_title, rdflib.BNode()))

                        body_hash_uri = aa[urllib.parse.quote(body_hash)]
                        body_hash_lit = Literal(body_hash)
                        g.add((body_hash_uri, RDF.type, aa.bodyHash))
                        g.add((url_uri, aa.has_body_hash, body_hash_lit))
                        g.add((url_uri, aa.has_body_hash, body_hash_lit))

                        # Add timestamps as Literals
                        if scraped_at is not None:
                            g.add((url_uri, aa.scraped_at, Literal(scraped_at, datatype=XSD.dateTime)))
                        else:
                            g.add((url_uri, aa.scraped_at, BNode()))

                        if inserted_at is not None:
                            g.add((url_uri, aa.inserted_at, Literal(inserted_at, datatype=XSD.dateTime)))
                        else:
                            g.add((url_uri, aa.inserted_at, BNode()))

                        if updated_at is not None:
                            g.add((url_uri, aa.updated_at, Literal(updated_at, datatype=XSD.dateTime)))
                        else:
                            g.add((url_uri, aa.updated_at, BNode()))

                        # Create dictionary for JSON string of news title and body
                        news_body_dct.update({url: {'domain': domain,
                                                    'title_hash': title_hash,
                                                    'title': title,
                                                    'body_hash': body_hash,
                                                    'body': body}})

            # Serialise RDF graph into TTL file string (to write to TTL file later)
            ttl_str = g.serialize(destination=None, format='ttl').decode('utf-8')
            # Turn dict to JSON string
            json_str = json.dumps(news_body_dct, indent=4)
            yield file_path, ttl_str, (url, domain, body_hash, body), json_str, out_path_ttl, out_path_json

        else:
            yield file_path, None, None, None, out_path_ttl, out_path_json


def run(argv=None, save_main_session=True):
    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

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
    parser.add_argument('--runner', default='FlinkRunner', required=True)
    parser.add_argument('--flink_master', default='localhost_8081', required=True)
    parser.add_argument('--environment_type', default='LOOPBACK', required=True)
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

    # Create a tuple file listing (input path, output path for TTL, output path for JSON text body)
    file_ls = [(input_file_path, os.path.join(known_args.output, os.path.basename(input_file_path)) + '.ttl',
                os.path.join(known_args.output, os.path.basename(input_file_path)) + '_text_body.json')
               for input_file_path in file_ls]

    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Beam pipeline to read all batched JSON files, then parse data into list of tuples
    # Each tuple is a news article (file path, title, news content, hash of news content)
    with beam.Pipeline(options=pipeline_options) as p:
        readable_files = (p
                          | 'Create PCollection' >> beam.Create(file_ls)
                          | 'Shuffle' >> beam.transforms.util.Reshuffle()
                          | 'Make RDFLib graph' >> beam.ParDo(MakeRDFLibGraph())
                          | 'Serialise RDFLib graph' >> beam.ParDo(SerializeRDFLibGraph())
                          | 'Parallel write Turtles' >> beam.ParDo(WriteTurtleJSON())
                          )

    result = p.run()
    result.wait_until_finish()

    end_time = datetime.datetime.now()
    print('Completed at: {}'.format(end_time))
    duration = end_time - start_tm
    print('Duration: {}'.format(duration))

    print('END')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
