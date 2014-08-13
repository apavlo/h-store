#!/bin/bash
BENCH=("winhstore" "winhstorenocleanup" "winhstorenostate" "winsstore")
OLD="wXsYY"
NEWW=("100" "1000" "10000")
NEWS=("1" "5" "10" "100")
TFILE="/tmp/out.tmp.$$"
for d in "${BENCH[@]}"
do
cd $d
for w in "${NEWW[@]}"
do
for s in "${NEWS[@]}"
do
REP="w${w}s${s}"
rm -rf $REP
cp -r $OLD $REP
mv "$REP/voter${d}${OLD}-ddl.sql" "$REP/voter${d}${REP}-ddl.sql"
mv "$REP/voter${d}${OLD}.mappings" "$REP/voter${d}${REP}.mappings"

DPATH="$REP/*"
for f in $DPATH
do
  if [ -f $f -a -r $f ]; then
   sed "s/$OLD/$REP/g" "$f" > $TFILE && mv $TFILE "$f"
  else
   echo "Error: Cannot read $f"
  fi
done
DPATH="$REP/procedures/*"
for f in $DPATH
do
  if [ -f $f -a -r $f ]; then
   sed "s/$OLD/$REP/g" "$f" > $TFILE && mv $TFILE "$f"
  else
   echo "Error: Cannot read $f"
  fi
done
sed "s/WINDOW_SIZE = 100/WINDOW_SIZE = $w/g" "$REP/VoterWinHStoreConstants.java" > $TFILE && mv $TFILE "$REP/VoterWinHStoreConstants.java"
sed "s/SLIDE_SIZE = 10/SLIDE_SIZE = $s/g" "$REP/VoterWinHStoreConstants.java" > $TFILE && mv $TFILE "$REP/VoterWinHStoreConstants.java"
sed "s/ROWS 100 SLIDE 10/ROWS $w SLIDE $s/g" "$REP/voterwinsstore${REP}-ddl.sql" > $TFILE && mv $TFILE "$REP/voterwinsstore${REP}-ddl.sql"

rm "../properties/voter${d}${REP}.properties"
cp "../properties/voter${d}${OLD}.properties" "../properties/voter${d}${REP}.properties"
sed "s/${OLD}/${REP}/g" "../properties/voter${d}${REP}.properties" > $TFILE && mv $TFILE "../properties/voter${d}${REP}.properties"

done
done
cd ..
done
