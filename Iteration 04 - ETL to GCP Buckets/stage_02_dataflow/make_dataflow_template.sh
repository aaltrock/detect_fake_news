gsutil rm gs://src_fake_news_python/template1
gsutil -m rm -r gs://src_fake_news_python/tmp
source /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow/venv/bin/activate
cd /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow/ 
python3 main.py --region europe-west2 --input gs://fake_news_cleaned_json/*.* --output gs://fake_news_ttl_json --runner DataflowRunner --project detect-fake-news-313201 --temp_location gs://src_fake_news_python/tmp/ --template_location gs://src_fake_news_python/template1 --requirements_file ./requirements_stage_2_gcp_dataflow.txt
