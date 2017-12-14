#!/bin/bash
# This scripts ran the sample and accelerator data file specifically for mico dataset  against grami.
# Code is on utopia prayaas machine and this script is run on utopia because graph is installed there only.
for (( r=10; r <= 50 ; r=r+10 ))
do
for (( a=10; a <= 50 ; a=a+10 ))
do
      cmd="./grami -f mico_range_"$r"_accelerator_"$a"test.lg -s 160 -t 1 -p 1 -d 200"
$cmd
cmd="mv Output.txt Output_range_accelerator_"$r"_"$a".txt"
$cmd
done

done


#One more loop because i made mistake of naming files inconsistently.
for (( r=10; r <= 50 ; r=r+10 ))
do
      cmd="./grami -f mico_range_"$r"_accelerator_1test.lg -s 160 -t 1 -p 1 -d 200"
$cmd
cmd="mv Output.txt Output_range_accelerator_"$r"_1_mico.txt"
$cmd
done
