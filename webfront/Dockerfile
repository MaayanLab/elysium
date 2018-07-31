FROM ubuntu:16.04

MAINTAINER Alexander Lachmann <alexander.lachmann@mssm.edu>

# Python installs
RUN apt-get update && apt-get install -y \
    python \
    python-dev \
    python-pip \
    python-setuptools \
    libmysqlclient-dev

# pip installs
RUN pip install --upgrade pip
RUN pip install tornado
RUN pip install MySQL-python
RUN pip install requests
RUN pip install boto
RUN pip install python-dateutil --upgrade
RUN pip install awscli --upgrade --user

WORKDIR /usr/local/src

RUN mkdir -p /app/tornado/data
COPY . /app/tornado

EXPOSE 5000

WORKDIR /app/tornado

RUN chmod -R 777 /app/tornado
ENTRYPOINT ./entrypoint.sh
