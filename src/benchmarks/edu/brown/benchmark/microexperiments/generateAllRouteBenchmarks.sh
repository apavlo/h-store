#!/bin/bash
BENCH=("routetrig" "noroutetrig" "routetrigclient")
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
mv "$REP/microexp${d}${OLD}-ddl.sql" "$REP/microexp${d}${REP}-ddl.sql"
mv "$REP/microexp${d}${OLD}.mappings" "$REP/microexp${d}${REP}.mappings"

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

x=$((w+1))
for i in `seq ${x} 10`
do
  sed "s#Proc${i}.class#//Proc${i}.class#g" "$REP/RouteTrigProjectBuilder.java" > $TFILE && mv $TFILE "$REP/RouteTrigProjectBuilder.java"
done

sed "s/NUM_TRIGGERS = 0/NUM_TRIGGERS = $w/g" "$REP/RouteTrigConstants.java" > $TFILE && mv $TFILE "$REP/RouteTrigConstants.java"

rm "../properties/microexp${d}${REP}.properties"
cp "../properties/microexp${d}.properties" "../properties/microexp${d}${REP}.properties"
sed "s/${OLD}/${REP}/g" "../properties/microexp${d}${REP}.properties" > $TFILE && mv $TFILE "../properties/microexp${d}${REP}.properties"

done
cd ..
done
