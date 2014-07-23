#!/bin/bash
TFILE="/tmp/out.tmp.$$"
OLD="HStore"
NEW="SStore"
DPATH="wXsYY/*"
for f in $DPATH
do
  if [ -f $f -a -r $f ]; then
   sed "s/$OLD/$NEW/g" "$f" > $TFILE && mv $TFILE "$f"
  else
   echo "Error: Cannot read $f"
  fi
done

DPATH="wXsYY/procedures/*"
for f in $DPATH
do
  if [ -f $f -a -r $f ]; then
   sed "s/$OLD/$NEW/g" "$f" > $TFILE && mv $TFILE "$f"
  else
   echo "Error: Cannot read $f"
  fi
done

OLD="hstore"
NEW="sstore"
DPATH="wXsYY/*"
for f in $DPATH
do
  if [ -f $f -a -r $f ]; then
   sed "s/$OLD/$NEW/g" "$f" > $TFILE && mv $TFILE "$f"
  else
   echo "Error: Cannot read $f"
  fi
done

DPATH="wXsYY/procedures/*"
for f in $DPATH
do
  if [ -f $f -a -r $f ]; then
   sed "s/$OLD/$NEW/g" "$f" > $TFILE && mv $TFILE "$f"
  else
   echo "Error: Cannot read $f"
  fi
done