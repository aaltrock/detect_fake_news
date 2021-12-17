# Shell script to run Python script on the GraphDB host to ingest the TTL files obtained from GCP Cloud Storage (specifically for NER pipeline)
source ./venv/bin/activate
python ./03_upload_graphdb.py --input=gs://src_fake_news_bs/added_ttl_json --local_ingest_path=/home/aaron_altrock/graphdb-import/ --graphdb_address=localhost:7200 --graphdb_repo=ner_fake_news --sync_shell_path=/home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow_bs/Iteration\ 6/Features\ Engineering/NER_pipeline/sync_graph_import_folder_ner.sh

