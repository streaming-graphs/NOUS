args <- commandArgs(trailingOnly = TRUE)
dataset = read.table("/sumitData/work/myprojects/AIM/branch_master/Mining/output/frequentPatternsTopK.tsv",sep="\t")
#dataset = read.table(args[1],sep = "\t")
setEPS()
postscript("/sumitData/work/myprojects/AIM/branch_master/Mining/output/frequentPatternsTopK.eps")
barplot(dataset$V2,names.arg = dataset$V1,las=2,cex.names=0.6)
dev.off()

