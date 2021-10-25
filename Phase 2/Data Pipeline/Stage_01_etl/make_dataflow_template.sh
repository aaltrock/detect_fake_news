# Remove any existing template file created
gsutil rm gs://src_fake_news_bs/template1

# Remove any temporary directory and content
gsutil -m rm -r gs://src_fake_news_bs/tmp

# Active Python virtual environment
source /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow_bs/venv/bin/activate

# Change to the base directory
cd /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow_bs/

# Run to compile into a template file using GCP's DataFlowRunner
python3 01_gcp_bucket_to_bigquery_dataflow.py --region europe-west2 \
--input gs://src_fake_news_bs/to_add/*.parquet \
--output gs://fake_news_ttl_json \
--runner DataflowRunner \
--project fake-news-bs-detector \
--staging_location=gs://fake_news_cleaned_json/staging \
--temp_location gs://src_fake_news_bs/tmp/ \
--template_location gs://src_fake_news_bs/template1 \
--requirements_file ./requirements_01_gcp_bucket_to_bigquery_dataflow.txt \
--source_bucket_name src_fake_news_bs \
--source_bucket_base_path to_add \
--processed_bucket_success added \
--bigquery_output_dataset fake_news \
--bigquery_output_table src_fake_news \
--json_key_path ./fake-news-bs-detector-62e838f6b99c.json

