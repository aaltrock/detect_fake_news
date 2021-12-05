# Script to create a new GCP DataFlow job - run in GCP Cloud Shell
# Usage example:
# ./make_gcp_dataflow_job.sh docker-stage-01-benchmarking-europe-west2 1250 europe-west2

export JOB_NAME=$1
export SAMPLE_SIZE=$2
export REGION=$3

# Reset by moving the files back from the processed to the 'to be processed' area in Cloud Storage
gsutil mv gs://src_fake_news_bs/added/*.* gs://src_fake_news_bs/to_add/

# Create a GCP DataFlow job to run according to job name, sample size and GCP region
gcloud dataflow jobs run $JOB_NAME --gcs-location gs://src_fake_news_bs/template1_$SAMPLE_SIZE --region $REGION --staging-location gs://src_fake_news_bs/tmp
