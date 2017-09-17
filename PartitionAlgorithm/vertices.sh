#!/bin/sh

#######Script to extract vertices.dat from the input file#######

cat $1 | awk '{print $1}' >> tmp1
cat $1 | awk '{print $2}' >> tmp2
cat tmp1 >> tmp2
cat tmp2 | sort | uniq >> tmp3
wc -l tmp3 | awk '{print $1}' >> vertices.dat
cat tmp3 >> vertices.dat
