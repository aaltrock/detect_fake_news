import requests
import os
import datetime
from subprocess import call


# Func to call Graph DB via API to load a server-side file to a given URL of a graph db
def post_to_graphdb(file_nm, graph_db_url='http://localhost:7200/rest/data/import/server/src_fake_news'):
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
    # Copy the .ttl files from saved location of Beam pipeline to the designated import location for graph db
    beam_data_path = r'/Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow/beam_ttl_json/'
    graph_db_import_path = r'/Users/aaronaltrock/graphdb-import/'
    call('cp -p ' + beam_data_path + '*.ttl ' + graph_db_import_path, shell=True)

    # Capture start time timestamp
    start_tm = datetime.datetime.now()
    print('Started at: {}'.format(start_tm))

    # Get list of files saved to the graphdb-import directory
    files_ls = os.listdir(graph_db_import_path)

    # Exclude config.ttl GraphDB config file
    files_ls = [files_nm for files_nm in files_ls if files_ls != 'config.ttl']
    print('List of files in {} excluding config.ttl:'.format(graph_db_import_path))
    print(files_ls)

    # For each TTL file, run call POST to import TTL content to graph DB
    for file_nm in files_ls:
        res = post_to_graphdb(file_nm)
        print('Graph DB called to import {} with response code: {}'.format(file_nm, res))

    end_time = datetime.datetime.now()
    print('Completed at: {}'.format(end_time))
    duration = end_time - start_tm
    print('Duration: {}'.format(duration))
    print('END')


if __name__ == '__main__':
    run()
