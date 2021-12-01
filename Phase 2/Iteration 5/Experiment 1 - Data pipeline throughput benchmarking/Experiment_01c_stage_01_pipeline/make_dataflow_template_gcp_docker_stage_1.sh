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
../venv/bin/python 01_gcp_bucket_to_bigquery_dataflow.py --region=europe-west2 --input=gs://src_fake_news_bs/to_add/risdal_$SAMPLE_SIZE.parquet --runner=DataflowRunner --job_endpoint=embed --project=fake-news-bs-detector --staging_location=gs\:\/\/src_fake_news_bs\/staging --temp_location=gs://src_fake_news_bs/tmp/ --template_location=gs://src_fake_news_bs/template1_$SAMPLE_SIZE --source_bucket_name=src_fake_news_bs --source_bucket_base_path=to_add --processed_bucket_success=added --bigquery_output_dataset=fake_news --bigquery_output_table=src_fake_news_$SAMPLE_SIZE --json_key_path=./fake-news-bs-detector-62e838f6b99c.json --experiments=use_runner_v2 --sdk_container_image=$IMAGE_URI

