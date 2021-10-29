

# Ref: https://cloud.google.com/dataflow/docs/guides/using-custom-containers#docker
export GOOGLE_APPLICATION_CREDENTIALS=/Users/aaronaltrock/PycharmProjects/detect_fake_news_data_flow_bs/fake-news-bs-detector-62e838f6b99c.json
export PROJECT=fake-news-bs-detector
export REPO=europe-west2-docker.pkg.dev/fake-news-bs-detector/dataflow-docker-stage-01
export TAG=latest
export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG
export $SERVICE_ID=srv-df@fake-news-bs-detector.iam.gserviceaccount.com

sudo gcloud auth activate-service-account $SERVICE_IDM --key-file=$GOOGLE_APPLICATION_CREDENTIALS --project=$PROJECT_ID
gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://gcr.io

docker build . --tag $IMAGE_URI
docker push $IMAGE_URI


#DOCKER_BUILDKIT=0 docker build . --tag $IMAGE_URI
#docker push $IMAGE_URI

#DOCKER_BUILDKIT=0 docker build -t apache/beam_python3.7_sdk:2.27.0_custom .
