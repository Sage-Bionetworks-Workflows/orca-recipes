FROM apache/airflow:2.5.0-python3.10

# #update pip
RUN pip install --user --upgrade pip

#install requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt --no-deps
