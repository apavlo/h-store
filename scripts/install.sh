#!/bin/bash

BASE_DIR=`readlink -f .`

BROWN_BASE="SVN-Brown"
BROWN_SVN_PATH="http://graffiti.cs.brown.edu/svn/hstore"
BROWN_SRC_DIR="src"
BROWN_TESTS_DIR="tests"
BROWN_JARS_DIR="jars"
BROWN_WORKLOADS_DIR="workloads"
BROWN_SCRIPTS_DIR="scripts"
BROWN_DESIGNS_DIR="designs"
BROWN_UPDATE_SCRIPT="svn-update.sh"

## ---------------------------------------------------------------
## SYMLINK CONFIGURATION
## ---------------------------------------------------------------

SYMLINK_SOURCE=( \
   $BROWN_WORKLOADS_DIR \
   $BROWN_SCRIPTS_DIR \
   $BROWN_DESIGNS_DIR \
)

## ---------------------------------------------------------------
## SVN DOWNLOAD
## ---------------------------------------------------------------

echo -e "\nDownloading from Brown repository..."
if [ ! -d "$BROWN_BASE" ]; then
   svn co $BROWN_SVN_PATH $BROWN_BASE
fi
echo "Done"

## ---------------------------------------------------------------
## CREATE PATH SYMLINKS
## ---------------------------------------------------------------

total=${#SYMLINK_SOURCE[@]}
count=0
cd $BROWN_BASE/$BROWN_SRC_DIR
while [ $count -lt $total ]; do
   source=${SYMLINK_SOURCE[$count]}
   target=`basename $source`

   echo -e "\nCreating $target symlink..."
   if [ ! -L $target ]; then
      ln -s ../$source .
      echo "Done"
   else
      echo "Skip"
   fi
   count=`expr $count + 1`
done
if [ ! -L "contrib-build.xml" ]; then
   ln -s src/frontend/mit_acr/build.xml contrib-build.xml
fi
cd -

echo -e "\nAll done! You smell bad!!!\n"
exit