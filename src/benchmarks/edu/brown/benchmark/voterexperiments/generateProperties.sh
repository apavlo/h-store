#!/bin/bash
OLD="w100s10"
NEW="wXsYY"
DPATH="wXsYY/*"
TFILE="/tmp/out.tmp.$$"
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
/bin/rm $TFILE

