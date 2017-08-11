awk -F',' '!($48 in F){F[$48]="nums_Output_"++i".csv"; print >F[$48]; close(F[$48]); next}{print >>F[$48]; close(F[$48])}' attack_nums.csv


awk -F',' '!($49 in F){F[$49]="Output_"++i".csv"; print >F[$49]; close(F[$49]); next}{print >>F[$49]; close(F[$49])}' nums_Output_1.csv
