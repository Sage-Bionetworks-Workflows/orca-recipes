FROM apache/airflow:2.5.0-python3.10


# # ARG GITHUB_ACCESS_TOKEN
# ENV GITHUB_ACCESS_TOKEN=${GITHUB_ACCESS_TOKEN}


# USER root
# #install git
# RUN apt-get -y update
# RUN apt-get -y install git

# USER airflow
# # GitHub
# RUN git config --global url."https://BWMac:${GITHUB_ACCESS_TOKEN}@github.com/bwmac".insteadof "https://github.com/bwmac"

# #update pip
RUN pip install --user --upgrade pip

#install requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt --no-deps
