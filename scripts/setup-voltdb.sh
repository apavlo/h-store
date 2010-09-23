#!/bin/bash

BASE_DIR=`readlink -f .`

VOLTDB_BASE="SVN-VoltDB"
VOLTDB_SVN_PATH="https://svn.voltdb.com/eng/trunk"

BROWN_BASE=`realpath "$1/src"` # SVN-Brown"
BROWN_SVN_PATH="http://graffiti.cs.brown.edu/svn/hstore"
BROWN_SRC_DIR="src"
BROWN_TESTS_DIR="tests"
BROWN_JARS_DIR="jars"
BROWN_WORKLOADS_DIR="workloads"
BROWN_SCRIPTS_DIR="scripts"
BROWN_DESIGNS_DIR="designs"
BROWN_UPDATE_SCRIPT="svn-update.sh"

## ---------------------------------------------------------------
## SVN CHECKOUT CONFIGURATION
## ---------------------------------------------------------------
BROWN_SVN_SOURCE=( \
   "$BROWN_SVN_PATH/build/src/frontend/mit_acr" \
   "$BROWN_SVN_PATH/build/third_party/java/jars/" \
   "$BROWN_SVN_PATH/build/tests/frontend/edu/brown" \
   "$BROWN_SVN_PATH/workloads/" \
   "$BROWN_SVN_PATH/scripts/" \
   "$BROWN_SVN_PATH/designs/" \
)
BROWN_SVN_TARGET=( \
   $BROWN_SRC_DIR \
   $BROWN_JARS_DIR \
   $BROWN_TESTS_DIR \
   $BROWN_WORKLOADS_DIR \
   $BROWN_SCRIPTS_DIR \
   $BROWN_DESIGNS_DIR \
)

## ---------------------------------------------------------------
## SYMLINK CONFIGURATION
## ---------------------------------------------------------------

SYMLINK_CHDIR=( \
   "$VOLTDB_BASE/src/frontend" \
   "$VOLTDB_BASE" \
   "$VOLTDB_BASE/tests/frontend/edu/" \
   "$VOLTDB_BASE" \
   "$VOLTDB_BASE" \
   "$VOLTDB_BASE" \
)

SYMLINK_SOURCE=( \
   "$BROWN_BASE/$BROWN_SRC_DIR" \
   "$BROWN_BASE/$BROWN_WORKLOADS_DIR" \
   "$BROWN_BASE/$BROWN_TESTS_DIR" \
   "$BROWN_BASE/$BROWN_SRC_DIR/frontend/gpl_vcr/build.xml" \
   "$BROWN_BASE/$BROWN_SCRIPTS_DIR" \
   "$BROWN_BASE/$BROWN_DESIGNS_DIR" \
)

SYMLINK_TARGET=( \
   "mit_acr" \
   "workloads" \
   "brown" \
   "contrib-build.xml" \
   "scripts" \
   "designs" \
)


## ---------------------------------------------------------------
## SVN DOWNLOAD
## ---------------------------------------------------------------

if [ ! -d "$BROWN_BASE" ]; then
   echo "ERROR: The directory '$BROWN_BASE' does not exist!"
   exit 1
fi

echo "Downloading from VoltDB repository..."
if [ ! -d "$VOLTDB_BASE" ]; then
   svn co $VOLTDB_SVN_PATH $VOLTDB_BASE
fi
echo "Done"

## ---------------------------------------------------------------
## CREATE PATH SYMLINKS
## ---------------------------------------------------------------

total=${#SYMLINK_CHDIR[@]}
count=0
while [ $count -lt $total ]; do
   dir=${SYMLINK_CHDIR[$count]}
   source=${SYMLINK_SOURCE[$count]}
   target=${SYMLINK_TARGET[$count]}

   echo -e "\nCreating $target symlink..."
   if [ ! -d $dir ]; then
      mkdir -p $dir
   fi
   cd $dir
   if [ ! -L $target ]; then
      ln -s $source $target
      echo "Done"
   else
      echo "Skip"
   fi
   cd $BASE_DIR
   count=`expr $count + 1`
done

## ---------------------------------------------------------------
## CREATE PATH SYMLINKS
## ---------------------------------------------------------------

echo -e "\nCreating jar symlinks..."
cd $VOLTDB_BASE/third_party/java/jars/
JARS=( \
   "collections-generic-4.01.jar" \
   "jung-algorithms-2.0.jar" \
   "jung-api-2.0.jar" \
   "jung-graph-impl-2.0.jar" \
   "jung-visualization-2.0.jar" \
   "tools.jar" \
)
for JAR in ${JARS[@]}; do
   if [ ! -L $JAR ]; then
      ln -s $BROWN_BASE/$BROWN_JARS_DIR/$JAR
   fi
done
cd $BASE_DIR
echo "Done"

## ---------------------------------------------------------------
## SVN UPDATE SCRIPT
## ---------------------------------------------------------------

# echo -e "\nCreating SVN updater script..."
# cd $BROWN_BASE
# 
# echo "#!/bin/sh -x" > $BROWN_UPDATE_SCRIPT
# total=${#BROWN_SVN_SOURCE[@]}
# count=0
# while [ $count -lt $total ]; do
#    target=${BROWN_SVN_TARGET[$count]}
#    echo "svn update $target" >> $BROWN_UPDATE_SCRIPT
#    count=`expr $count + 1`
# done
# echo "exit" >> $BROWN_UPDATE_SCRIPT
# chmod 0755 $BROWN_UPDATE_SCRIPT
# cd $BASE_DIR
# echo "Done"

echo -e "\nAll done! You smell bad!!!\n"
exit
