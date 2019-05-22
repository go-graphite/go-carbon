#!/bin/sh

cd `dirname $0`
ROOT=$PWD

docker run -ti --rm -v $ROOT:/root/go/src/github.com/lomik/go-carbon ubuntu:18.10 bash -c '
    cd /root/
    export GO_VERSION=1.10.3
    apt-get update
    apt-get install -y rpm ruby ruby-dev wget make git gcc

    wget https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz
    ln -s /usr/local/go/bin/go /usr/local/bin/go
    
    # newer fpm is broken https://github.com/jordansissel/fpm/issues/1612
    gem install rake
    gem install fpm:1.10.2

    go get github.com/mitchellh/gox
    ln -s /root/go/bin/gox /usr/local/bin/gox

    cd /root/go/src/github.com/lomik/go-carbon

    make gox-build
    make fpm-deb
    make fpm-rpm
'
