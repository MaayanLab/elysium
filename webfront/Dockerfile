FROM ubuntu:18.04

MAINTAINER Alexander Lachmann <alexander.lachmann@mssm.edu>

# Python installs
RUN apt-get update && apt-get install -y \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools

# pip installs
RUN pip3 install --upgrade pip
RUN pip3 install tornado
RUN pip3 install pymysql
RUN pip3 install requests
RUN pip3 install boto3
RUN pip3 install python-dateutil --upgrade
RUN pip3 install awscli --upgrade --user

WORKDIR /usr/local/src

RUN mkdir -p /app/tornado/data
COPY . /app/tornado

EXPOSE 5000

WORKDIR /app/tornado

RUN chmod -R 777 /app/tornado
ENTRYPOINT ./entrypoint.sh
