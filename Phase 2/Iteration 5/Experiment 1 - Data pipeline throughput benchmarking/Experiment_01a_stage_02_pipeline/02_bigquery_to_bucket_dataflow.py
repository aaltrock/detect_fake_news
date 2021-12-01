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

# DoFn class to write Turtle strings into TTL text files
class WriteTurtleJSON(beam.DoFn):
    def process(self, element):
        aa, g, out_path_ttl, ttl_str = element

        # Write RDF Graph to Turtle
        writer = fileio.filesystems.FileSystems.create(out_path_ttl)
        writer.write(bytes(ttl_str, encoding='utf-8'))
        writer.close()

        yield None


# DoFn class to create a RDFLib graph with name space, classesand objects to populate entities later
class MakeRDFLibGraph(beam.DoFn):

    def process(self, element, to_write_bucket_nm):
        # Instantiate RDFLib graph object to add triples
        g = rdflib.Graph()

        # Prefix and bind to graph
        aa = Namespace('http://www.city.ac.uk/ds/inm363/aaron_altrock#')
        g.bind('aa', aa)

        # items to parse in dict (except custom columns - to be processed separately)
        std_parse_items_ls = ['body_hash', 'file_name_hash', 'label', 'title_hash', 'url_hash',
                              'author_hash', 'country_of_origin', 'language',
                              'classification_date', 'publication_date']

        # Make JSON and TTL file name based on SHA 256 hash of the article_id and url_id concatenated
        webpage_id = element.get('article_id') + '_' + element.get('url_hash')
        webpage_id = webpage_id.encode('utf-8')
        webpage_id = hashlib.sha256(webpage_id).hexdigest()

        # Add webpage ID as the standard item
        std_parse_items_ls += ['webpage_id']
        element.update({'webpage_id': webpage_id})

        # Create the new .ttl and .json file based on hashed URL
        out_path_ttl = to_write_bucket_nm + '/' + webpage_id + '.ttl'

        # Parse custom values (if any) to dictionary from string
        try:
            custom_dct = json.loads(element.get('custom_columns'))
        except Exception as e:
            custom_dct = {}

        """
        Classes Triples
        """
        classes_ls = [aa.bodyHash, aa.urlHash,  aa.author, aa.title, aa.country,
                      aa.language, aa.classification, aa.publication, aa.infoSource, aa.extraClass]

        for cls in classes_ls:
            g.add((cls, RDF.type, OWL.Class))

        """
        Object Triples - Pack individual triples from JSON files content
        """
        object_ls = [aa.has_body_hash, aa.classified_by, aa.has_url_hash, aa.authored_by, aa.has_title_hash, aa.is_from_country,
                     aa.is_in_language, aa.has_classification, aa.is_published_by, aa.is_sourced_from,
                     aa.published_on, aa.classified_on, aa.has_extra_class, aa.is_extra_class]

        # # Add custom predicates
        # if len(list(custom_dct.keys())) > 0:
        #     for custom_col_nm in list(custom_dct.keys()):
        #         obj_nm = 'has_cf_' + custom_col_nm
        #         object_ls += [aa[obj_nm]]

        for obj in object_ls:
            g.add((obj, RDF.type, OWL.ObjectProperty))

        """
        Data Properties
        """
        data_prop_ls = [aa.publishedDate, aa.classifiedDate]

        # # Add custom fields as data properties
        # if len(list(custom_dct.keys())) > 0:
        #     data_prop_ls += [aa['cf_' + custom_field_nm] for custom_field_nm in list(custom_dct.keys())]

        # if len(list(custom_dct.keys())) > 0:
        #     for custom_col_nm in list(custom_dct.keys()):
        #         class_nm = 'cf' + custom_col_nm
        #         classes_ls += [aa[class_nm]]

        for data_prop in data_prop_ls:
            g.add((data_prop, RDF.type, OWL.DataProperty))

        """
        Add individuals
        """

        # Help func to coalesce values
        def __coalesce_val(dct, key):
            if dct.get(key) is not None:
                if str(dct.get(key)) != 'null':
                    return str(dct.get(key))
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
        pub_uri = aa[urllib.parse.quote(__coalesce_val(element, 'webpage_id'))]
        pub_lit = Literal(__coalesce_val(element, 'webpage_id'))
        g.add((pub_uri, RDF.type, aa.publication))
        g.add((pub_uri, RDFS.label, pub_lit))

        # body_hash map to body
        body_uri = aa[urllib.parse.quote(__coalesce_val(element, 'body_hash'))]
        body_lit = Literal(__coalesce_val(element, 'body_hash'))
        g.add((body_uri, RDF.type, aa.bodyHash))
        g.add((body_uri, RDFS.label, body_lit))
        g.add((pub_uri, aa.has_body_hash, body_uri))

        # file_name_hash map to classification
        file_name_uri = aa[urllib.parse.quote(__coalesce_val(element, 'file_name_hash'))]
        file_name_lit = Literal(__coalesce_val(element, 'file_name_hash'))
        g.add((file_name_uri, RDF.type, aa.infoSource))
        g.add((file_name_uri, RDFS.label, file_name_lit))
        g.add((pub_uri, aa.is_sourced_from, file_name_uri))

        # label
        label_uri = aa[urllib.parse.quote(__coalesce_val(element, 'label'))]
        label_lit = Literal(__coalesce_val(element, 'label'))
        g.add((label_uri, RDF.type, aa.Classification))
        g.add((label_uri, RDFS.label, label_lit))
        g.add((pub_uri, aa.has_classification, label_uri))

        # title_hash
        title_uri = aa[urllib.parse.quote(__coalesce_val(element, 'title_hash'))]
        title_lit = Literal(__coalesce_val(element, 'title_hash'))
        g.add((title_uri, RDF.type, aa.title))
        g.add((title_uri, RDFS.label, title_lit))
        g.add((pub_uri, aa.has_title_hash, title_uri))

        # url_hash
        url_uri = aa[urllib.parse.quote(__coalesce_val(element, 'url_hash'))]
        url_lit = Literal(__coalesce_val(element, 'uri_hash'))
        g.add((url_uri, RDF.type, aa.urlHash))
        g.add((url_uri, RDFS.label, url_lit))
        g.add((pub_uri, aa.has_url_hash, url_uri))

        # author_hash
        author_uri = aa[urllib.parse.quote(__coalesce_val(element, 'author_hash'))]
        author_lit = Literal(__coalesce_val(element, 'author_hash'))
        g.add((author_uri, RDF.type, aa.author))
        g.add((author_uri, RDFS.label, author_lit))
        g.add((pub_uri, aa.is_published_by, author_uri))

        # country_of_origin
        country_uri = aa[urllib.parse.quote(__coalesce_val(element, 'country_of_origin'))]
        country_lit = Literal(__coalesce_val(element, 'country_of_origin'))
        g.add((country_uri, RDF.type, aa.country))
        g.add((country_uri, RDFS.label, country_lit))
        g.add((pub_uri, aa.is_from_country, country_uri))

        # language
        lang_uri = aa[urllib.parse.quote(__coalesce_val(element, 'language'))]
        lang_lit = Literal(__coalesce_val(element, 'language'))
        g.add((lang_uri, RDF.type, aa.language))
        g.add((lang_uri, RDFS.label, lang_lit))
        g.add((pub_uri, aa.is_in_language, lang_uri))

        # classification_date as data property
        g.add((pub_uri, aa.classifiedDate,
               Literal(__coalesce_val(element, 'classification_date'), datatype=XSD.dateTime)))

        # publication_date as data property
        g.add((pub_uri, aa.publishedDate,
               Literal(__coalesce_val(element, 'publication_date'), datatype=XSD.dateTime)))

        """
        Extra classes
        """
        # Custom columns (if exists)
        cust_classes_ls = []
        if len(list(custom_dct.keys())) > 0:
            for custom_col_nm in list(custom_dct.keys()):
                class_nm = 'cf_' + custom_col_nm
                cust_classes_ls += [aa[class_nm]]

        for cust_cls in cust_classes_ls:
            g.add((cust_cls, RDF.type, OWL.Class))
            g.add((pub_uri, aa.has_extra_class, cust_cls))

        """
        Extra classes individuals
        """
        # Custom field individuals as data properties
        for cust_nm in custom_dct:
            val = custom_dct.get(cust_nm)
            data_type = __guess_data_type(val)

            # Determine the XSD data type
            if data_type is None:
                data_type = BNode()
            elif data_type == 'string':
                data_type = XSD.string
            elif data_type == 'integer':
                data_type = XSD.integer
            elif data_type == 'float':
                data_type = XSD.float
            elif data_type == 'datetime':
                data_type = XSD.dateTime
            else:
                data_type = XSD.string

            # Add custom individual as data property
            has_cust_nm = 'has_cf_' + cust_nm
            cust_cls_nm = 'cf_' + cust_nm
            if data_type is not None:
                cust_data_prop = aa[urllib.parse.quote('cf_' + cust_nm)]
                cust_uri = aa[urllib.parse.quote(__coalesce_val(custom_dct, cust_nm))]
                cust_lit = Literal(__coalesce_val(custom_dct, cust_nm), datatype=data_type)
                # g.add((cust_uri, RDF.type, aa[cust_cls_nm]))
                g.add((cust_data_prop, RDF.type, OWL.DataProperty))
                # g.add((cust_uri, RDFS.label, cust_lit))
                # g.add((pub_uri, aa[has_cust_nm], cust_uri))
                # g.add((cust_uri, aa.has_extra_class, aa.extraClass))
                g.add((pub_uri, aa.has_extra_class, aa.extraClass))
                g.add((pub_uri, cust_data_prop, cust_lit))
            else:
                cust_data_prop = aa[urllib.parse.quote('cf_' + cust_nm)]
                cust_uri = BNode()
                cust_lit = Literal(__coalesce_val(custom_dct, cust_nm))
                # g.add((cust_uri, RDF.type, aa[cust_cls_nm]))
                g.add((cust_data_prop, RDF.type, OWL.DataProperty))
                # g.add((cust_uri, RDFS.label, cust_lit))
                # g.add((pub_uri, aa[has_cust_nm], cust_uri))
                # g.add((cust_uri, aa.has_extra_class, aa.extraClass))
                g.add((pub_uri, aa.has_extra_class, aa.extraClass))
                g.add((pub_uri, cust_data_prop, cust_lit))


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
    parser.add_argument(
        '--sample_size',
        dest='sample_size',
        required=True,
        help='Benchmark sample size')

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
            FROM `fake-news-bs-detector.fake_news.src_fake_news_{}`
            GROUP BY article_id, url
        )
        SELECT 
        orig.*
        FROM LATEST_TIMESTAMP_CTE AS latest
        INNER JOIN `fake-news-bs-detector.fake_news.src_fake_news_{}` AS orig
        ON orig.article_id = latest.article_id
        AND orig.url = latest.url
        AND orig.etl_timestamp = latest.etl_timestamp;

    """.format(known_args.sample_size, known_args.sample_size)

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
                          | 'Push to GraphDB' >> beam.ParDo(WriteTurtleJSON())
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
