counter=0
dirToRead=110
for d in /mnt/openke/drone/*/;  do
 echo "\n*********Reading Directory " $d
 counter=$((counter+1))
 cmd="find $d"
 dirname=$(basename $d)
 #Get input file which has list of json files to process. one in each line
 #Testing Line: $cmd | head -n 3 > dronefile.list 
 $cmd > dronefile.list
 #Put that file to hdfs. run.sh scripts always looks for dronefile.list file
 cmd="hdfs dfs -put dronefile.list"
 $cmd
 #Run the job script. It will create dronefile.out directory in the hdfs
 cmd="sh /home/pnnl/NOUS/triple_extractor/run.sh"
 echo "\n*********Running the job using run.sh"
 $cmd
 #Copy dronefile.out to the name of base dir which is year number i.e. 20150124.out
 cmd="hdfs dfs -mv dronefile.out $dirname"."out"
 echo "\n*********Moving dronefile.out in HDFS to its year's name $dirname"
 $cmd
 #Copy dronfile.in to the name of base dir which is year number i.e. 20150124.list
 cmd="hdfs dfs -mv dronefile.list $dirname"."list"
 echo "\n*********Moving dronefile.list in HDFS to its year's name $dirname"
 $cmd
 cmd="mv dronefile.list $dirname"."list"
 echo "\n*********Moving dronefile.list From local path to its year's name $dirname"
 $cmd
 if [ $counter -eq $dirToRead ];  then
  exit
 fi
done
