# Script to run locally the Beam pipeline with the docker image (sourced from GCP Container Registry GCR)

#Ref: https://cloud.google.com/dataflow/docs/guides/using-custom-containers#docker
export PROJECT=fake-news-bs-detector
export REPO=europe-west2-docker.pkg.dev/fake-news-bs-detector/dataflow-docker-stage-01
export TAG=latest
export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG
./venv/bin/python 01_gcp_bucket_to_bigquery_dataflow.py --region=europe-west2 --input=gs://src_fake_news_bs/to_add/*.parquet --runner=PortableRunner --job_endpoint=embed --project=fake-news-bs-detector --staging_location=gs\:\/\/src_fake_news_bs\/staging --temp_location=gs://src_fake_news_bs/tmp/ --template_location=gs://src_fake_news_bs/template1 --source_bucket_name=src_fake_news_bs --source_bucket_base_path=to_add --processed_bucket_success=added --bigquery_output_dataset=fake_news --bigquery_output_table=src_fake_news --json_key_path=./fake-news-bs-detector-62e838f6b99c.json --environment_type=DOCKER --environment_config=$IMAGE_URI

