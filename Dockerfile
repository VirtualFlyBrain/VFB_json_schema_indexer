FROM python:3.8

ENV PDBserver=http://pdb.virtualflybrain.org
ENV PDBuser=neo4j
ENV PDBpassword=password
ENV OutputPath=./indexes/solr_index.json
ENV BatchSize=500


RUN mkdir /code /code/src/
ADD requirements.txt /code/

RUN pip install -r /code/requirements.txt
ADD src/indexers /code/src/indexers
ADD src/main.py /code/src/
ADD src/__init__.py /code/src/

WORKDIR /code

RUN echo "Installing VFB json schema" && \
cd /tmp && \
git clone --quiet https://github.com/VirtualFlyBrain/VFB_json_schema.git

RUN mkdir -p /code/src/vfb && \
mv /tmp/VFB_json_schema/src/* /code/src/vfb

RUN ls -l /code && ls -l /code/src && ls -l /code/src/vfb

ENTRYPOINT bash -c "cd /code; python3 src/main.py"