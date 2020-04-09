FROM ubuntu:18.04

RUN sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list && sed -i s@/security.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list

RUN apt-get update && apt-get install -y build-essential wget vim \
&& wget -qO- https://dl.google.com/go/go1.13.6.linux-amd64.tar.gz | tar xz -C /usr/local

ENV PATH=$PATH:/usr/local/go/bin
COPY 6.824-golabs-2020 /root/6.824
WORKDIR /root/6.824
