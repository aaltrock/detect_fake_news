
# Docker file to create docker container for NER pipeline
# Ref: https://cloud.google.com/dataflow/docs/guides/using-custom-containers#docker
# Before running, reference to the actual JSON secret file for the service account to login
export GOOGLE_APPLICATION_CREDENTIALS=fake-news-bs-detector-f954b0ce540d.json
export PROJECT=fake-news-bs-detector
export REPO=europe-west2-docker.pkg.dev/fake-news-bs-detector/dataflow-docker-ner
export TAG=latest
export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG
export SERVICE_ID=srv-df@fake-news-bs-detector.iam.gserviceaccount.com

# cp /Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow_bs/Features\ Engineering/NER_pipeline/ner_pipeline.py ./
# cp /Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow_bs/Features\ Engineering/NER_pipeline/App_NER_requirements.txt ./
# cp /Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow_bs/Features\ Engineering/NER_pipeline/fake-news-bs-detector-f954b0ce540d.json ./
sudo gcloud auth activate-service-account $SERVICE_IDM --key-file=$GOOGLE_APPLICATION_CREDENTIALS --project=$PROJECT_ID
gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://gcr.io

docker build . --tag $IMAGE_URI
docker push $IMAGE_URI
