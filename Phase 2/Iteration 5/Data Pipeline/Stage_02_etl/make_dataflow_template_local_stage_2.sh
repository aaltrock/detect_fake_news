# Stage 02 pipeline - Script to run locally the Beam pipeline with the docker image (sourced from GCP Container Registry GCR)

#Ref: https://cloud.google.com/dataflow/docs/guides/using-custom-containers#docker
export PROJECT=fake-news-bs-detector
export REPO=europe-west2-docker.pkg.dev/fake-news-bs-detector/dataflow-docker-stage-02
export TAG=latest
export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG
./venv/bin/python ./02_bigquery_to_bucket_dataflow.py --input=gs://src_fake_news_bs/added --output=gs://src_fake_news_bs/added_ttl_json --runner=PortableRunner --job_endpoint=embed --project=fake-news-bs-detector --staging_location=gs://src_fake_news_bs/staging_02/ --temp_location=gs://src_fake_news_bs/tmp/ --template_location=gs://src_fake_news_bs/template2 --source_bucket_name=src_fake_news_bs --bigquery_dataset=fake_news --bigquery_table=src_fake_news --json_key_path=./fake-news-bs-detector-62e838f6b99c.json --environment_type=DOCKER --environment_config=$IMAGE_URI

