#!/bin/bash
# This scripts ran the range + sample and hold data file against grami.
# Code is on utopia prayaas machine and this script is run on utopia because graph is installed there only.
for (( r=10; r <= 50 ; r=r+10 ))
do

for (( q=2; q <=8 ; q=q+2 ))
do
for (( p=2; p <=8 ; p=p+2 ))
do
      cmd="./grami -f citeseer_sample_range_"$r"_samplehold_q_0."$q"_p_0."$p".lg -s 160 -t 1 -p 1 -d 200"
$cmd
cmd="mv Output.txt Output_range_"$r"_samplehold_q_0."$q"_p_0."$p".txt"
$cmd
done
done
done


#One more loop because i made mistake of naming files inconsistently.
for (( r=10; r <= 50 ; r=r+10 ))
do

for (( p=2; p <=8 ; p=p+2 ))
do
      cmd="./grami -f citeseer_sample_range_"$r"_samplehold_q_1.0_p_0."$p".lg -s 160 -t 1 -p 1 -d 200"
$cmd
cmd="mv Output.txt Output_range_"$r"_samplehold_q_1.0_p_0."$p".txt"
$cmd
done
done





#One more loop because i made mistake of naming files inconsistently.
for (( r=10; r <= 50 ; r=r+10 ))
do

for (( q=2; q <=8 ; q=q+2 ))
do
      cmd="./grami -f citeseer_sample_range_"$r"_samplehold_q_0."$q"_p_1.0.lg -s 160 -t 1 -p 1 -d 200"
$cmd

cmd="mv Output.txt Output_range_"$r"_samplehold_q_0."$q"_p_1.0.txt"
$cmd
done
done
