import argparse
import requests
import os
import datetime
from subprocess import call
import re


# Func to call Graph DB via API to load a server-side file to a given URL of a graph db
def post_to_graphdb(file_nm, graph_db_url):
    # Header for the POST requests to GraphDB
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    # Data payload
    data = '{ "fileNames": ["' + file_nm + '"] }'

    # Call POST request to instruct GraphDB to upload the specified file to the graph database src_fake_news
    response = requests.post(graph_db_url, headers=headers, data=data)

    return response


# Main execution
def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        help='input bucket location')
    parser.add_argument(
        '--local_ingest_path',
        dest='local_ingest_path',
        default=r'/home/aaron_altrock/graphdb-import/',
        help='Local path where GraphDB ingest TTL files')
    parser.add_argument(
        '--graphdb_address',
        dest='graphdb_address',
        default='localhost:7200',
        help='GraphDB IP address including the IP address and port number')
    parser.add_argument(
        '--graphdb_repo',
        dest='graphdb_repo',
        default='src_fake_news',
        help='GraphDB repository name')
    parser.add_argument(
        '--sync_shell_path',
        dest='sync_shell_path',
        help='Path to the Bash shell script to sync with GCP Cloud storage for the TTL documents',
        default='/home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow_bs/sync_graph_import_folder.sh')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Make URL to the REST API for the ingestion, e.g. 'http://localhost:7200/rest/data/import/server/src_fake_news'
    graph_db_url = 'http://' + known_args.graphdb_address + '/rest/data/import/server/' + known_args.graphdb_repo

    # Capture start time timestamp
    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Copy the .ttl files from saved location of Beam pipeline to the designated import location for graph db
    graph_db_import_path = known_args.local_ingest_path
    print('Running shell command to copy the DataFlow output files from GCP to local before ingestion to GraphDB')
    call(known_args.sync_shell_path, shell=True)
    print('Completed')

    # Get list of files saved to the graphdb-import directory
    files_ls = os.listdir(graph_db_import_path)

    # Exclude config.ttl GraphDB config file and .ttl format
    files_ls = [files_nm for files_nm in files_ls if re.search('.ttl$', files_nm) is not None]
    print('No of files in {} to upload to GraphDB: {}'.format(graph_db_import_path, len(files_ls)))

    # For each TTL file, run call POST to import TTL content to graph DB
    for file_nm in files_ls:
        res = post_to_graphdb(file_nm, graph_db_url)
        print('Graph DB called to import {} with response code: {}'.format(file_nm, res), end='\r')

    end_time = datetime.datetime.now()
    print('Completed at: {}'.format(end_time))
    duration = end_time - start_tm
    print('Duration: {}'.format(duration))
    print('END')


if __name__ == '__main__':
    run()
