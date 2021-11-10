# Script to run and create template file to GCP Cloud Storage using the docker image (from Google Container Register) to run GCP DataFlow.

#Ref: https://cloud.google.com/dataflow/docs/guides/using-custom-containers#docker

# Usage example for creating a Flex template for a sample size of 1,250.  Template generated will bear the suffix _1250.
# ./make_data_template_gcp_docker_stage_1.sh 1250

export SAMPLE_SIZE=$1
export PROJECT=fake-news-bs-detector
export REPO=europe-west2-docker.pkg.dev/fake-news-bs-detector/dataflow-docker-stage-01
export TAG=latest
export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG
export JSON_KEY=fake-news-bs-detector-62e838f6b99c.json

# Move the file back from processed area to the to be added area (if any)
export GOOGLE_APPLICATION_CREDENTIALS=JSON_KEY
#gcloud auth login --cred-file=$JSON_KEY
sudo gsutil mv gs://src_fake_news_bs/added/*.* gs://src_fake_news_bs/to_add/

# Start a GCP DataFlow job based on the sample size
../venv/bin/python 02_bigquery_to_bucket_dataflow.py --region=europe-west2 --input=gs://src_fake_news_bs/added --output=gs://src_fake_news_bs/added_ttl_json_$SAMPLE_SIZE --runner=DataflowRunner --job_endpoint=embed --project=fake-news-bs-detector --staging_location=gs://src_fake_news_bs/staging_02/ --temp_location=gs://src_fake_news_bs/tmp/ --template_location=gs://src_fake_news_bs/template2_$SAMPLE_SIZE --source_bucket_name=src_fake_news_bs --bigquery_dataset=fake_news --bigquery_table=src_fake_news --json_key_path=$JSON_KEY --environment_type=DOCKER --environment_config=$IMAGE_URI --sample_size=$SAMPLE_SIZE
