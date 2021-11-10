# Shell script to run Python script on the GraphDB host to ingest the TTL files obtained from GCP Cloud Storage.
source ./venv/bin/activate
python ./03_upload_graphdb.py --input=gs://src_fake_news_bs/added_ttl_json --local_ingest_path=/home/aaron_altrock/graphdb-import/ --graphdb_address=localhost:7200 --graphdb_repo=src_fake_news --sync_shell_path=/home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow_bs/sync_graph_import_folder.sh

