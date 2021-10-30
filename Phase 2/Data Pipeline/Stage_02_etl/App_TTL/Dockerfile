# Docker file to create docker container for Stage 2 pipeline
FROM apache/beam_python3.7_sdk:2.27.0

# Copy all Python script
COPY 02_bigquery_to_bucket_dataflow.py /02_bigquery_to_bucket_dataflow.py

# Copy JSON token
COPY fake-news-bs-detector-62e838f6b99c.json /fake-news-bs-detector-62e838f6b99c.json

# Copy requirements file
COPY requirements_02_bigquery_to_bucket_dataflow.txt /requirements_02_bigquery_to_bucket_dataflow.txt

ENV GOOGLE_APPLICATION_CREDENTIALS=/fake-news-bs-detector-62e838f6b99c.json

# Run install
RUN pip install pip -U
RUN pip install setuptools -U
RUN pip install -r ./requirements_02_bigquery_to_bucket_dataflow.txt
RUN pip list
