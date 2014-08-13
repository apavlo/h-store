#!/bin/bash
BENCH=("ftriggers" "noftriggers")
OLD="orig"
NEWN=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")
TFILE="/tmp/out.tmp.$$"
for d in "${BENCH[@]}"
do
echo $d
cd $d
for w in "${NEWN[@]}"
do
echo $w
REP="trig${w}"
rm -rf $REP
cp -r $OLD $REP
mv "$REP/microexp${d}-ddl.sql" "$REP/microexp${d}${REP}-ddl.sql"
mv "$REP/microexp${d}.mappings" "$REP/microexp${d}${REP}.mappings"

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
sed "s/NUM_TRIGGERS = 0/NUM_TRIGGERS = $w/g" "$REP/FTriggersConstants.java" > $TFILE && mv $TFILE "$REP/FTriggersConstants.java"
sed "s/NUM_TRIGGERS = 0/NUM_TRIGGERS = $w/g" "$REP/NoFTriggersConstants.java" > $TFILE && mv $TFILE "$REP/NoFTriggersConstants.java"

rm "../properties/microexp${d}${REP}.properties"
cp "../properties/microexp${d}.properties" "../properties/microexp${d}${REP}.properties"
sed "s/${OLD}/${REP}/g" "../properties/microexp${d}${REP}.properties" > $TFILE && mv $TFILE "../properties/microexp${d}${REP}.properties"

done
cd ..
done
