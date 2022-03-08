#!/bin/sh

cd `dirname $0`
ROOT=$PWD

docker run -ti --rm -v $ROOT:/root/go/src/github.com/go-graphite/go-carbon ubuntu:20.04 bash -c '
    cd /root/
    export GO_VERSION=1.17
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get update
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y rpm ruby ruby-dev wget build-essential

    wget https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz
    ln -s /usr/local/go/bin/go /usr/local/bin/go

    # newer fpm is broken https://github.com/jordansissel/fpm/issues/1612
    gem install rake fpm:1.10.2 package_cloud

    go install github.com/mitchellh/gox@latest
    ln -s /root/go/bin/gox /usr/bin/gox

    cd /root/go/src/github.com/go-graphite/go-carbon

    make gox-build
    make fpm-deb
    make fpm-rpm
'
