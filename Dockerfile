FROM apache/airflow:2.5.1-python3.10

# Install Git for orca package installation (before release)
USER root
RUN apt update && apt install git -y
USER airflow

# #update pip
RUN pip install --user --upgrade pip

#install requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt --no-deps
