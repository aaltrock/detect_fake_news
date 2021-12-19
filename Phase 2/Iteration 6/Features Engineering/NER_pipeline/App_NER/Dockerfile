# Docker file to create docker container for Stage 2 pipeline
FROM apache/beam_python3.7_sdk:2.27.0

# Copy all Python script
COPY ./ner_pipeline.py /ner_pipeline.py

# Copy JSON token
COPY ./fake-news-bs-detector-f954b0ce540d.json /fake-news-bs-detector-f954b0ce540d.json

# Copy requirements file
COPY ./App_NER_requirements.txt /App_NER_requirements.txt

ENV GOOGLE_APPLICATION_CREDENTIALS=/fake-news-bs-detector-f954b0ce540d.json

# Run install
RUN apt update -y
RUN apt install -y python3-pip
RUN python3 -m pip install pip -U
RUN python3 -m pip install setuptools -U
RUN pip3 install wheel -U

# GraphDB specific
RUN pip3 install -r App_NER_requirements.txt
RUN pip3 install rdflib==4.2.2

# Space NER specifics 
RUN pip install -U spacy
RUN #python -m spacy download xx_ent_wiki_sm

RUN pip3 list
RUN pip3 list | grep -i rdflib