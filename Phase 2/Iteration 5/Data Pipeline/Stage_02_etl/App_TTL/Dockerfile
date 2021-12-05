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
RUN apt update -y
RUN apt install -y python3-pip
RUN python3 -m pip install pip -U
RUN python3 -m pip install setuptools -U
RUN pip3 install -r requirements_02_bigquery_to_bucket_dataflow.txt
RUN pip3 install rdflib==4.2.2
RUN pip3 list
RUN pip3 list | grep -i rdflib
