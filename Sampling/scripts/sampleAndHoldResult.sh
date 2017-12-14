#!/bin/bash
for (( p=2; p <= 8 ; p=p+2 ))
do
for (( q=2; q <= 8 ; q=q+2 ))
do
      cmd="./grami -f citeseer_0."$p"_0."$q".lg -s 160 -t 1 -p 1 -d 200"
$cmd
cmd="mv Output.txt Output_0."$p"_0."$q".txt"
$cmd
done

done


#One more loop because i made mistake of naming files inconsistently.
for (( q=2; q <= 8 ; q=q+2 ))
do
      cmd="./grami -f citeseer_1_0."$q".lg -s 160 -t 1 -p 1 -d 200"
$cmd
cmd="mv Output.txt Output_1_0."$q".txt"
$cmd
done
