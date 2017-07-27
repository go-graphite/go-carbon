#!/bin/sh

cd `dirname $0`
docker build -t go-carbon-build .

cd ../../
ROOT=`pwd`

docker run -ti --rm -v $ROOT:/src/go-carbon/ go-carbon-build bash -c "
    set -xe
    cd /src/go-carbon/
    make gox-build
    make fpm-deb
    make fpm-rpm
    bash
"
