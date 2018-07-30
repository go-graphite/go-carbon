FROM ubuntu:18.04

RUN apt-get update && apt-get install -y rpm ruby ruby-dev git wget libffi-dev make gcc bzip2

RUN wget -o /dev/null https://storage.googleapis.com/golang/go1.10.3.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.10.3.linux-amd64.tar.gz && \
    ln -s /usr/local/go/bin/go /usr/local/bin/go && \
    go get github.com/mitchellh/gox && \
    mv /root/go/bin/gox /usr/local/bin/gox && \
    gem install fpm -v 1.8.1
