#!/bin/sh -x

CWD=`pwd`
BUILD=$1
if [ -z "$BUILD" ]; then
   echo "ERROR: Must pass in build target (release, debug)"
   exit 1
fi

INSTALL_PATH="obj/$BUILD/evan-dtxn"
FIRST=0
if [ -d $INSTALL_PATH ]; then
    cd $INSTALL_PATH
else
    mkdir -p $INSTALL_PATH
    cd $INSTALL_PATH
    hg clone http://people.csail.mit.edu/evanj/hg/index.cgi/stupidbuild
    hg clone http://people.csail.mit.edu/evanj/hg/index.cgi/hstore
    FIRST=1
fi

for repo in stupidbuild hstore; do
    cd $repo
    if [ "$FIRST" = 1 ]; then
        echo "
[diff]
git = 1
[extensions]
hgext.mq =" >> .hg/hgrc
    fi
    cd ..
done

cd hstore
hg qimport buildhack-linux.diff
hg qpush || exit 1

## Additional Patches
if [ `echo $HOSTNAME | grep -c cs.wisc.edu` = "1" ]; then
    hg qimport $CWD/scripts/dtxn-wisc-build.diff || exit 1
    hg qpush || exit 1
    hg qimport $CWD/scripts/dtxn-logging.diff || exit 1
    hg qpush || exit 1
fi

if [ "$FIRST" = 1 ]; then
    perl -pi -e 's/\~\/stupidbuild\//..\/stupidbuild\//' buildhack.sh
fi

## Build this mofo
./buildhack.sh

exit