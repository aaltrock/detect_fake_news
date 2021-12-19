"""
02 Script to read in from BigQuery to parse into Turtle Doc with the RDF triples
"""

import argparse
import logging
import os
import json
import datetime
import rdflib
import nltk
from rdflib.namespace import Namespace
from rdflib.namespace import OWL, RDF, RDFS, XSD
from rdflib import URIRef, BNode, Literal
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import hashlib
import urllib
import dateutil
import re
import spacy as sp
import xx_ent_wiki_sm

# DoFn class to write Turtle strings into TTL text files
class WriteTurtleJSON(beam.DoFn):
    def process(self, element):
        aa, g, out_path_ttl, ttl_str = element

        # Write RDF Graph to Turtle
        writer = fileio.filesystems.FileSystems.create(out_path_ttl)
        writer.write(bytes(ttl_str, encoding='utf-8'))
        writer.close()

        yield None


class RunNER(beam.DoFn):
    def process(selfself, element):

        # Compile a dict for all keys with string values for NER
        for_ner_dct = {}
        for key in list(element.keys()):
            # If dict key is not a hash and the individual and not 'null' is a string type include for NER
            if isinstance(element.get(key), str) and re.search(r'_hash$', key) is None and \
                    element.get(key, '') != 'null':
                for_ner_dct.update({key: element.get(key)})

        # Load multi-language Spacy Wiki small trained set
        nlp = sp.load('xx_ent_wiki_sm')

        # For each column with non-null string value get entities identified and re-pack into a dict each
        ent_ls = []     # List of entites to capture the NER entities
        for col_nm, corp in for_ner_dct.items():
            doc = nlp(corp)

            # Parse entities as string
            for ent in doc.ents:
                ent_ls += [(ent.text, ent.label_, col_nm)]

        # Remove duplicate entities
        ent_ls = list(set(ent_ls))

        res_ls = []
        for ent, label, col_nm in ent_ls:
            res_ls += [(element.get('article_id'),  # Article ID
                        element.get('url_hash'),    # URL Hash
                        col_nm,     # Column sourced for entity
                        ent,        # NER entity text
                        label,       # NER entity label
                        element.get('publication_date'),    # Publication date
                        col_nm  # Column found the NER
                        )]

        return res_ls


# DoFn class to create a RDFLib graph with name space, classesand objects to populate entities later
class MakeRDFLibGraph(beam.DoFn):

    def process(self, element, to_write_bucket_nm):
        # Instantiate RDFLib graph object to add triples
        g = rdflib.Graph()
        article_id = element[0]
        url_hash = element[1]
        ner_soruce_nm = element[2]
        ner_entity = element[3]
        ner_label = element[4]
        publication_dt = element[5]
        ner_src = element[6]

        # Prefix and bind to graph
        aa = Namespace('http://www.city.ac.uk/ds/inm363/aaron_altrock#')
        g.bind('aa', aa)

        # Make JSON and TTL file name based on SHA 256 hash of the article_id and url_id concatenated
        webpage_id = article_id + '_' + url_hash
        webpage_id_hash = webpage_id
        webpage_id_hash = webpage_id_hash.encode('utf-8')
        webpage_id_hash = hashlib.sha256(webpage_id_hash).hexdigest()

        ner_entity_hash = ner_entity
        ner_entity_hash = ner_entity_hash.encode('utf-8')
        ner_entity_hash = hashlib.sha256(ner_entity_hash).hexdigest()

        # Create the new .ttl and .json file based on hashed URL
        out_file_nm = 'NER_' + webpage_id_hash + '_' + ner_entity_hash + '_' + ner_label + '.ttl'
        out_path_ttl = to_write_bucket_nm + '/' + out_file_nm

        """
        Classes Triples
        """
        classes_ls = [aa.publication, aa.ner_entity, aa.ner_label, aa.ner_source]

        for cls in classes_ls:
            g.add((cls, RDF.type, OWL.Class))

        """
        Object Triples - Pack individual triples from JSON files content
        """
        object_ls = [aa.has_ner_entity, aa.has_ner_label, aa.has_url_hash, aa.has_ner_label,
                     aa.is_in_language]

        for obj in object_ls:
            g.add((obj, RDF.type, OWL.ObjectProperty))

        """
        Data Properties
        """
        data_prop_ls = [aa.ner_source]

        for data_prop in data_prop_ls:
            g.add((data_prop, RDF.type, OWL.DataProperty))

        """
        Add individuals
        """

        # Help func to coalesce values
        def __coalesce_val(val):
            if val is not None:
                if str(val) != 'null':
                    return str(val)
                else:
                    return BNode()
            else:
                return BNode()

        # Helper func to guess data type
        def __guess_data_type(val):
            # Default to string data type
            data_type = 'string'

            # If value is None or 'null' return no data type
            if val is None or val == 'null' or val == '':
                return None
            else:
                val = str(val)

            try:
                val = int(val)
                return 'integer'
            except:
                data_type = 'string'

            try:
                val = float(val)
                return 'float'
            except:
                data_type = 'string'

            try:
                val = dateutil.parser(val)
                return 'datetime'
            except:
                date_type = 'string'

            return date_type


        # publication
        pub_uri = aa[urllib.parse.quote(__coalesce_val(webpage_id))]
        g.add((pub_uri, RDF.type, aa.publication))

        # NER entity
        ner_entity_uri = aa[urllib.parse.quote(__coalesce_val(ner_entity))]
        ner_entity_lit = Literal(__coalesce_val(ner_entity))
        g.add((ner_entity_uri, RDF.type, aa.ner_entity))
        g.add((ner_entity_uri, RDFS.label, ner_entity_lit))
        g.add((pub_uri, aa.has_ner_entity, ner_entity_uri))

        # NER label
        ner_label_uri = aa[urllib.parse.quote(__coalesce_val(ner_label))]
        ner_label_lit = Literal(__coalesce_val(ner_label))
        g.add((ner_label_uri, RDF.type, aa.ner_label))
        g.add((ner_label_uri, RDFS.label, ner_label_lit))
        g.add((ner_entity_uri, aa.has_ner_label, ner_label_uri))

        # file_name_hash map to classification
        file_name_uri = aa[urllib.parse.quote(out_file_nm)]
        g.add((file_name_uri, RDF.type, aa.infoSource))
        g.add((pub_uri, aa.is_sourced_from, file_name_uri))
        g.add((ner_entity_uri, aa.is_sourced_from, file_name_uri))

        # publication_date as data property
        g.add((ner_entity_uri, aa.ner_source,
               Literal(__coalesce_val(ner_src))))

        # Serialise Turtle to string
        ttl_str = g.serialize(destination=None, format='ttl').decode('utf-8')

        # Write RDF Graph to Turtle
        writer = fileio.filesystems.FileSystems.create(out_path_ttl)
        writer.write(bytes(ttl_str, encoding='utf-8'))
        writer.close()

        yield aa, g, out_path_ttl, ttl_str


# # DoFn class to serialise RDFLib graph after filling in with entities into Turtle strings
# class SerializeRDFLibGraph(beam.DoFn):
#
#     def process(self, element):
#         aa = element[0]
#         g = element[1]
#         out_path_ttl = element[2]
#
#         ttl_str = g.serialize(destination=None, format='ttl').decode('utf-8')
#
#         yield aa, g, out_path_ttl, ttl_str


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
    pipeline_args += ['--project', known_args.project,
                      '--output', known_args.output,
                      '--bigquery_dataset', known_args.bigquery_dataset,
                      '--bigquery_table', known_args.bigquery_table]

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
                          | 'NER' >> beam.ParDo(RunNER())
                          | 'Make RDFLib graph' >> beam.ParDo(MakeRDFLibGraph(), known_args.output)
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
