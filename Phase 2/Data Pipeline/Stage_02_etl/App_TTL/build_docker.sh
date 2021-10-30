
# Docker file to create docker container for Stage 2 pipeline
# Ref: https://cloud.google.com/dataflow/docs/guides/using-custom-containers#docker
export GOOGLE_APPLICATION_CREDENTIALS=/Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow_bs/fake-news-bs-detector-62e838f6b99c.json
export PROJECT=fake-news-bs-detector
export REPO=europe-west2-docker.pkg.dev/fake-news-bs-detector/dataflow-docker-stage-02
export TAG=latest
export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG
export $SERVICE_ID=srv-df@fake-news-bs-detector.iam.gserviceaccount.com

cp /Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow_bs/02_bigquery_to_bucket_dataflow.py ./
cp /Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow_bs/requirements_02_bigquery_to_bucket_dataflow.txt ./
cp /Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow_bs/fake-news-bs-detector-62e838f6b99c.json ./
sudo gcloud auth activate-service-account $SERVICE_IDM --key-file=$GOOGLE_APPLICATION_CREDENTIALS --project=$PROJECT_ID
gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://gcr.io

docker build . --tag $IMAGE_URI
docker push $IMAGE_URI
