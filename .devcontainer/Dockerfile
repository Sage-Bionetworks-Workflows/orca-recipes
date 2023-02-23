FROM mcr.microsoft.com/devcontainers/python:0-3.10

RUN pip install --no-cache-dir pipenv

ARG PIPFOLDER=/tmp/pipfile/

COPY ../Pipfile* "$PIPFOLDER"

RUN cd "$PIPFOLDER" \
  && pipenv install --dev --system

CMD ["bash"]
