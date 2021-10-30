"""
02 Script to read in from BigQuery to parse into Turtle Doc with the RDF triples
"""

import argparse
import logging
import os
import json
import datetime
import rdflib
from rdflib.namespace import Namespace
from rdflib.namespace import OWL, RDF, RDFS, XSD
from rdflib import URIRef, BNode, Literal
import urllib
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import hashlib


# DoFn class to write Turtle strings into TTL text files
class WriteTurtleJSON(beam.DoFn):
    def process(self, element):
        url_hash = element[0]
        ttl_str = element[1]
        url_hash, url, domain, domain_hash, body_hash, body = element[2]
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

    def process(self, element, to_write_bucket_nm):
        # Instantiate RDFLib graph object to add triples
        g = rdflib.Graph()

        # Prefix and bind to graph
        aa = Namespace('http://www.city.ac.uk/ds/inm363/aaron_altrock#')
        g.bind('aa', aa)

        # Item to parse as data attributes in standard items
        std_parse_data_attr_ls = []

        # items to prase in dict (except custom columns - to be processed separately)
        std_parse_items_ls = ['body_hash', 'file_name_hash', 'label', 'title_hash', 'url_hash',
                              'author_hash', 'country_of_origin', 'detailed_news_label', 'language',
                              'etl_timestamp', 'classification_date', 'publication_date']

        # Derive reference IDs
        article_id = element.get('article_id')
        url_hash = element.get('url_hash')

        # Make JSON and TTL file name based on SHA 256 hash of the article_id and url_id concatenated
        webpage_id = article_id + '_' + url_hash
        webpage_id = webpage_id.encode('utf-8')
        webpage_id = hashlib.sha256(webpage_id).hexdigest()

        # Create a refernece IDs
        ref_id_ls = ['webpage_id']

        # Add webpage ID as the standard item
        std_parse_items_ls += ['webpage_id']
        element.update({'webpage_id': webpage_id})

        # Create the new .ttl and .json file based on hashed URL
        out_path_ttl = to_write_bucket_nm + '/' + webpage_id + '.ttl'

        # Parse custom values (if any) to dictionary from string
        try:
            custom_dct = json.loads(element.get('custom_columns').decode('UTF-8'))
        except Exception as e:
            custom_dct = {}

        # Parse custom value keys
        parse_cust_items_ls = list(custom_dct.keys())

        """
        Classes Triples
        """
        # Define classes used in triples
        classes_ls = [aa[col_nm] for col_nm in std_parse_items_ls]
        if len(parse_cust_items_ls) > 0:
            classes_ls += [aa[cust_nm] for cust_nm in parse_cust_items_ls]  # Add custom items (if any)

        # Add as OWL class for each standard and custom columns from the BigQuery table
        # Add custom field to explicitly declare the classes that are customised
        for cls in classes_ls + [aa['custom_field']]:
            g.add((cls, RDF.type, OWL.Class))

        """
        Object Triples - Pack individual triples from JSON files content
        """
        object_ls = [aa['has_' + cls_nm] for cls_nm in classes_ls] + [aa['has_custom_field']]
        for obj in object_ls:
            g.add((obj, RDF.type, OWL.ObjectProperty))

        """
        Data Properties
        """
        data_prop_ls = [aa[data_attr_nm] for data_attr_nm in std_parse_data_attr_ls]
        for data_prop in data_prop_ls:
            g.add((data_prop, RDF.type, OWL.DataProperty))

        """
        Add individuals
        """
        for item_nm in std_parse_items_ls:
            val = element.get(item_nm)

            """
            Make subjects and objects
            """
            if val is not None and val != 'null':
                # Make URI
                item_uri = aa[val]

                # Make literal
                item_lit = Literal(val)
            else:
                # Impute the 'null' value
                if val is None:
                    val = 'null'
                # Make URI as Blank Node
                item_uri = BNode()

                # Make literal as 'null'
                item_lit = Literal(val)

            # Add Literal and URI declaration for item
            g.add((item_uri, RDF.type, aa[item_nm]))
            g.add((item_uri, RDFS.label, item_lit))

            # Map reference IDs to the item
            for ref_id in ref_id_ls:
                g.add((aa[ref_id], aa['has_' + item_nm], aa[item_nm]))

        yield aa, g, out_path_ttl


# DoFn class to serialise RDFLib graph after filling in with entities into Turtle strings
class SerializeRDFLibGraph(beam.DoFn):

    def process(self, element):
        aa = element[0]
        g = element[1]
        out_path_ttl = element[2]

        ttl_str = g.serialize(destination=None, format='ttl').decode('utf-8')

        yield aa, g, out_path_ttl, ttl_str


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default=os.path.join(os.getcwd(), 'parse_dict'),
        help='input bucket')
    parser.add_argument(
        '--output',
        dest='output',
        default=os.path.join(os.getcwd(), 'parse_dict'),
        help='output bucket location')
    parser.add_argument(
        '--bigquery_dataset',
        dest='bigquery_dataset',
        required=True,
        help='BigQuery dataset.')
    parser.add_argument(
        '--bigquery_table',
        dest='bigquery_table',
        required=True,
        help='BigQuery table within the dataset to source.')
    parser.add_argument(
        '--json_key_path',
        dest='json_key_path',
        required=True,
        help='Location where JSON key file holds.')
    parser.add_argument(
        '--project',
        dest='project',
        required=True,
        help='GCP project ID')


    known_args, pipeline_args = parser.parse_known_args(argv)

    # Add known arguments to pipeline arguments
    pipeline_args += ['--project', known_args.project, '--output', known_args.output]

    # Authenticate to GCP
    # Set environment variable to denote the location of the JSOn holding the authentication key
    # key_json_path = known_args.json_key_path
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = known_args.json_key_path

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Query to obtain the latest snapshot of all ingested articles from BigQuery
    query = """
        WITH LATEST_TIMESTAMP_CTE AS (
            SELECT  
            article_id
            ,url
            ,MAX(etl_timestamp) AS etl_timestamp
            FROM `fake-news-bs-detector.fake_news.src_fake_news`
            GROUP BY article_id, url
        )
        SELECT 
        orig.*
        FROM LATEST_TIMESTAMP_CTE AS latest
        INNER JOIN `fake-news-bs-detector.fake_news.src_fake_news` AS orig
        ON orig.article_id = latest.article_id
        AND orig.url = latest.url
        AND orig.etl_timestamp = latest.etl_timestamp;

    """

    # Authenticate into GCP BigQuery
    # Ref: https://cloud.google.com/bigquery/docs/authentication/service-account-file
    credentials = service_account.Credentials.from_service_account_file(
        known_args.json_key_path, scopes=['https://www.googleapis.com/auth/cloud-platform'],
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )

    # Beam pipeline to read all batched JSON files, then parse data into list of tuples
    # Each tuple is a news article (file path, title, news content, hash of news content)
    with beam.Pipeline(options=pipeline_options) as p:
        readable_files = (p
                          | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query,
                                                                             project=known_args.project,
                                                                             use_standard_sql=True)
                          | 'Shuffle' >> beam.transforms.util.Reshuffle()
                          | 'Make RDFLib graph' >> beam.ParDo(MakeRDFLibGraph(), known_args.output)
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
