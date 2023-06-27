FROM python:3.9.5
ENV REFRESHED_AT 2021-08-15


WORKDIR /home
ADD . /home

RUN apt-get update && \
  apt-get install -y --no-install-recommends gcc git libssl-dev g++ make && \
  cd /tmp && git clone https://github.com/edenhill/librdkafka && \
  cd librdkafka && git checkout tags/v2.0.2 && \
  ./configure && make && make install && \
  ldconfig &&\
  cd ../ && rm -rf librdkafka

RUN pip install -r requirements.txt

