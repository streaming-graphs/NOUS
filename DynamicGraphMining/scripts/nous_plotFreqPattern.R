args <- commandArgs(trailingOnly = TRUE)
#dataset = read.table("/sumitData/work/myprojects/AIM/branch_master/DynamicGraphMining/output/frequentPatterns.tsv",sep="\t")
dataset = read.table(args[1],sep = "\t")
setEPS()
postscript(args[2])
barplot(dataset$V2,names.arg = dataset$V1,las=2)
dev.off()

