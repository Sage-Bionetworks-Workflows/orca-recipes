ARG BASE_IMAGE=apache/airflow:2.7.2-python3.10
FROM $BASE_IMAGE

RUN pip install --no-cache-dir pipenv

ARG PIPFOLDER=/tmp/pipfile/

COPY Pipfile* "$PIPFOLDER"

RUN cd "$PIPFOLDER" \
  && pipenv install --dev --system
