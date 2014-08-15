#!/bin/bash
BENCH=("windows" "nowindows")
OLD="orig"
NEWW=("10" "100" "1000" "10000" "100000")
NEWS=("1" "2" "5" "10" "30" "100")
TFILE="/tmp/out.tmp.$$"
for d in "${BENCH[@]}"
do
cd $d
for w in "${NEWW[@]}"
do
REP="w${w}s2"
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
sed "s/WINDOW_SIZE = 100/WINDOW_SIZE = $w/g" "$REP/NoWindowsConstants.java" > $TFILE && mv $TFILE "$REP/NoWindowsConstants.java"
sed "s/SLIDE_SIZE = 1/SLIDE_SIZE = 2/g" "$REP/NoWindowsConstants.java" > $TFILE && mv $TFILE "$REP/NoWindowsConstants.java"
sed "s/ROWS 100 SLIDE 1/ROWS $w SLIDE 2/g" "$REP/microexpwindows${REP}-ddl.sql" > $TFILE && mv $TFILE "$REP/microexpwindows${REP}-ddl.sql"

rm "../properties/microexp${d}${REP}.properties"
cp "../properties/microexp${d}.properties" "../properties/microexp${d}${REP}.properties"
sed "s/${OLD}/${REP}/g" "../properties/microexp${d}${REP}.properties" > $TFILE && mv $TFILE "../properties/microexp${d}${REP}.properties"

done

for s in "${NEWS[@]}"
do
REP="w100s${s}"
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
sed "s/WINDOW_SIZE = 100/WINDOW_SIZE = 100/g" "$REP/NoWindowsConstants.java" > $TFILE && mv $TFILE "$REP/NoWindowsConstants.java"
sed "s/SLIDE_SIZE = 1/SLIDE_SIZE = $s/g" "$REP/NoWindowsConstants.java" > $TFILE && mv $TFILE "$REP/NoWindowsConstants.java"
sed "s/ROWS 100 SLIDE 1/ROWS 100 SLIDE $s/g" "$REP/microexpwindows${REP}-ddl.sql" > $TFILE && mv $TFILE "$REP/microexpwindows${REP}-ddl.sql"

rm "../properties/microexp${d}${REP}.properties"
cp "../properties/microexp${d}.properties" "../properties/microexp${d}${REP}.properties"
sed "s/${OLD}/${REP}/g" "../properties/microexp${d}${REP}.properties" > $TFILE && mv $TFILE "../properties/microexp${d}${REP}.properties"

done


cd ..
done
