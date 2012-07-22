#!/usr/bin/env perl

use strict;
use warnings;

my $YEAR=`date +%Y`;
chomp($YEAR);

my $COPYRIGHT = <<END;
/***************************************************************************
 *  Copyright (C) $YEAR by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
END

die("ERROR: Missing target directory\n") unless ($#ARGV >= 0);
my $TARGET_DIR = $ARGV[0];
die("ERROR: Invalid target directory $TARGET_DIR\n") unless (-d $TARGET_DIR);

foreach my $file (`find $TARGET_DIR -name "*.java" -type f`) {
   chomp($file);
   my $line = `head -n 20 $file`;
   unless ($line =~ m/Copyright \(C\) [\d]{4,4} .*?/) {
      my $contents = $COPYRIGHT.`cat $file`;
      open(FILE, ">$file") or die;
      print FILE $contents;
      close(FILE);
      print "Updated $file\n";
   } ## UNLESS
} # FOREACH
# print `svn status $TARGET_DIR`;
exit;