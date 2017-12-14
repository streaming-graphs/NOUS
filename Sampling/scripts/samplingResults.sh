#!/bin/bash
for (( filenamePrefix=1; filenamePrefix <= 10; filenamePrefix++ ))
do
      cmd="./grami -f citeseer_"$filenamePrefix"0.lg -s 160 -t 1 -p 1 -d 200"
$cmd
cmd="mv Output.txt Output_"$filenamePrefix".txt"
$cmd


done
