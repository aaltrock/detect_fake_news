FROM apache/beam_python3.7_sdk:2.27.0

# Copy all Python script
COPY 01_gcp_bucket_to_bigquery_dataflow.py /01_gcp_bucket_to_bigquery_dataflow.py

# Copy JSON token
COPY fake-news-bs-detector-62e838f6b99c.json /fake-news-bs-detector-62e838f6b99c.json

# Copy requirements file
COPY requirements_01_gcp_bucket_to_bigquery_dataflow.txt /requirements_01_gcp_bucket_to_bigquery_dataflow.txt

ENV GOOGLE_APPLICATION_CREDENTIALS=/fake-news-bs-detector-62e838f6b99c.json

# Run install
RUN pip install pip -U
RUN pip install setuptools -U
RUN pip install -r ./requirements_01_gcp_bucket_to_bigquery_dataflow.txt
RUN pip list
