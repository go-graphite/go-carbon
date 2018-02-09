#!/bin/sh

PACKAGE="github.com/lomik/go-carbon"

cd `dirname $0`
ROOT=`pwd`

rm -rf _vendor/src
mkdir -p $ROOT/_vendor/src/`dirname $PACKAGE`
cd $ROOT/_vendor/src/`dirname $PACKAGE`
LINK=`python -c "import os.path; print os.path.relpath('$ROOT', '$PWD')"`
ln -s $LINK `basename $PACKAGE`

cat $ROOT/Gopkg.lock | grep "  name = " | awk -F '"' '{print $2}' | while read line ; do 
    mkdir -p $ROOT/_vendor/src/`dirname $line`
    cd $ROOT/_vendor/src/`dirname $line`
    VENDOR=$ROOT/vendor/$line
    rm -f `basename $line`
    LINK=`python -c "import os.path; print os.path.relpath('$VENDOR', '$PWD')"`
    ln -s $LINK `basename $line`
    echo $line
done