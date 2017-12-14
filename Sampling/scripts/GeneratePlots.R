# Global variables
require(MESS)
#legends <- c("Random Sampling","RangeWithAccelerator","SampleHold","RangeWithSampleHold","ForestFire", "RSH_Diversity", "RSH_PageRank", "RSH_DiversiyV2", "RSH_PageRV3", "RSH_Triangle" );
#colorScheme <- c("red","blue","green","black","magenta","orange","seagreen", "skyblue","tomato","red");


#generate generic plots 2 in one row
plotXYCSVFileGeneric <- function(csvInputfilepath,ouputEPSPath,
                                 xrange, yrange, 
                                 legendXPos,legendYPos,
                                 numberOfLegends ){
  legends <- c("PageRank", "Triangle","Diversiy" ,"NodeRank");
  colorScheme <- c( "skyblue","tomato","seagreen", "black");
   
  # Init output device EPS 
  setEPS()
  postscript(ouputEPSPath)
  
  
  # Get dataset
  dataset = read.csv(csvInputfilepath,check.names=FALSE);
  allowedColumns <- c(7,1,3,5)
  allowedLegendIndex <- c(4,1,2,3)
  
  column <- 1
  columnNames = colnames(dataset)
  relevantLegends = legends[allowedLegendIndex]
  relevantColorSchement = colorScheme[allowedLegendIndex]
  xr <- c(1,xrange)
  yr <- c(1,yrange)
  plot(xr,yr,type="n",xlab=columnNames[column],ylab=columnNames[column+1])  
  grid()
  legend(legendXPos, legendYPos, legend=relevantLegends, col=relevantColorSchement, lty=1:1, cex=0.8,box.lty=0,y.intersp=1.5)
   
  
  for(column in allowedColumns){
    x<-dataset[,column]
    y<-dataset[,column+1]
    index <- ((column-1)/2)+1
    currentColor <- colorScheme[index];
    xy <- cbind(x,y);
    validxy <- subset(xy,(x>40 & !is.na(y)))
    
    validx <- validxy[,"x"]
    validy <- validxy[,"y"]
    line <- paste(auc(validx,validy),currentColor)
    write(line,file=paste(ouputEPSPath,".out",sep = ""),append=TRUE)
    
    #lines(x,y,col=currentColor)
    points(x,y,pch=4,col=currentColor)
    column <- column + 2
  }
  dev.off()
  return(1)
}



#res <- plotXYCSVFile("/sumitData/work/PhD/GraphSampling/Reports/citeseerAllRunTime.csv",
#                     "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/citeseer_runtime3.eps",100,100,5,95)

#res <- plotXYCSVFile("/sumitData/work/PhD/GraphSampling/Reports/citeseerAllAccuracy.csv",
#                     "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/citeseer_accuracy_generic1.eps",100,23,5,20)

#res <- plotXYCSVFileGeneric("/sumitData/work/PhD/GraphSampling/Reports/citeseerAllAccuracy.csv",
 #                    "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/citeseer_accuracy_generic1.eps",100,23,5,20)

#res <- plotXYCSVFileGeneric("/sumitData/work/PhD/GraphSampling/Reports/VLDBKDDCIKMAccuracy.csv",
 #                           "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/VLDBKDDCIKMAccuracy.eps",100,3354,5,3000)


#res <- plotXYCSVFile("/sumitData/work/PhD/GraphSampling/Reports/citeseerAllAccuracyIteration30.csv",
#                     "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/citeseer_accuracyItr30.eps",100,23,5,20)

#res <- plotXYCSVFile("/sumitData/work/PhD/GraphSampling/Reports/powergridAllAccuracy.csv",
#                     "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/powergrid_accuracy2_try2fixed.eps",100,900,5,895)

#res <- plotXYCSVFile("/sumitData/work/PhD/GraphSampling/Reports/powergridAllRunTime.csv",
#                      "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/powergrid_runtime3.eps",100,100,5,95)

legends <- c("S50_D10","S100_D10","S150_D10","S200_D10");
res <- plotXYCSVFile("/sumitData/work/PhD/GraphSampling/Reports/p2pAllAccuracy.csv",
             "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/p2p_accuracy3.eps",100,100,5,95,4)
#Reset legens again
#legends <- c("Random Sampling","RangeWithAccelerator","SampleHold","RangeWithSampleHold");

#legends <- c("S250_D50","S2000_D50","S4000_D50");
#res <- plotXYCSVFile("/sumitData/work/PhD/GraphSampling/Reports/p2pDenseAllAccuracy.csv", "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/p2p_dense_accuracy2.eps", 100,100,5,95,3)
#Reset legens again
#legends <- c("Random Sampling","RangeWithAccelerator","SampleHold","RangeWithSampleHold");
 
#legends <- c("L2 Communities",	"L3 Communities",	"L4 Communities");
#res <- plotXY1YnCSVFile("/sumitData/work/PhD/GraphSampling/Reports/citeseerComDetectionAllAccuracy.csv", "/sumitData/work/PhD/GraphSampling/publications/sampling_graph_mining/citeseerCD_accuracy2.eps", 100,100,5,95,3)
#Reset legends again
#legends <- c("Random Sampling","RangeWithAccelerator","SampleHold","RangeWithSampleHold");



plotXYZCSVFile <- function(csvInputfilepath,ouputEPSPath, xrange, yrange, legendXPos,legendYPos ){
  # Not fully functional code
  # Init output device EPS
  setEPS()
  postscript(ouputEPSPath)
  
  # Get dataset
  csvInputfilepath <- "/sumitData/work/PhD/GraphSampling/Reports/SampleHold_pq_graphsize.csv"
  dataset = read.csv(csvInputfilepath,check.names=FALSE);
  p<-dataset[,1][1:5]
  q<-dataset[,2][1:5]
  z<-matrix(dataset[,5],nrow = 5, ncol = 5,byrow=TRUE)
  c(p,q,z)
  dev.off()
  return(1)
  
}

#SampleHold_pq_graphsize
plotXYCSVFile <- function(csvInputfilepath,ouputEPSPath,
                          xrange, yrange, 
                          legendXPos,legendYPos,
                          numberOfLegends ){
  # Init output device EPS
  setEPS()
  postscript(ouputEPSPath)
  
  numberOfColumnsToIterate <- 0
  if(missing(numberOfLegends))
    numberOfColumnsToIterate <- 18
  else
    numberOfColumnsToIterate <- 2 * numberOfLegends
    
  
  # Get dataset
  dataset = read.csv(csvInputfilepath,check.names=FALSE);

  column <- 1
  columnNames = colnames(dataset)
  xr <- c(1,xrange)
  yr <- c(1,yrange)
  plot(xr,yr,type="n",xlab=columnNames[column],ylab=columnNames[column+1])  
  grid()
  legend(legendXPos, legendYPos, legend=legends, col=colorScheme, lty=1:1, cex=0.8,box.lty=0,y.intersp=1.5)
  
  while (column < numberOfColumnsToIterate){
    x<-dataset[,column]
    y<-dataset[,column+1]
    index <- ((column-1)/2)+1
    currentColor <- colorScheme[index];
    xy <- cbind(x,y);
    validxy <- subset(xy,(x>40 & !is.na(y)))
    
    validx <- validxy[,"x"]
    validy <- validxy[,"y"]
    line <- paste(auc(validx,validy),currentColor)
    write(line,file=paste(ouputEPSPath,".out",sep = ""),append=TRUE)
    
    #lines(x,y,col=currentColor)
    points(x,y,pch=4,col=currentColor)
    column <- column + 2
  }
  dev.off()
  return(1)
}

plotXYZCSVFile <- function(csvInputfilepath,ouputEPSPath, xrange, yrange, legendXPos,legendYPos ){
  # Not fully functional code
  # Init output device EPS
  setEPS()
  postscript(ouputEPSPath)
  
  # Get dataset
  csvInputfilepath <- "/sumitData/work/PhD/GraphSampling/Reports/SampleHold_pq_graphsize.csv"
  dataset = read.csv(csvInputfilepath,check.names=FALSE);
  p<-dataset[,1][1:5]
  q<-dataset[,2][1:5]
  z<-matrix(dataset[,5],nrow = 5, ncol = 5,byrow=TRUE)
  c(p,q,z)
  dev.off()
  return(1)
}

plotXY1YnCSVFile <- function(csvInputfilepath,ouputEPSPath,
                          xrange, yrange, 
                          legendXPos,legendYPos,
                          numberOfLegends ){
  # Init output device EPS
  setEPS()
  postscript(ouputEPSPath)
  
  numberOfColumnsToIterate <- 0
  if(missing(numberOfLegends))
    numberOfColumnsToIterate <- 3
  else
    numberOfColumnsToIterate <- numberOfLegends
  
  numberOfColumnsToIterate
  print("value of com to iter")
  # Get dataset
  dataset = read.csv(csvInputfilepath,check.names=FALSE);
  
  column <- 1
  columnNames = colnames(dataset)
  xr <- c(1,xrange)
  yr <- c(1,yrange)
  plot(xr,yr,type="n",xlab=columnNames[column],ylab=columnNames[column+1])  
  grid()
  legend(legendXPos, legendYPos, legend=legends, col=colorScheme, lty=1:1, cex=0.8,box.lty=0,y.intersp=1.5)
  
  x<-dataset[,1] #x is always the first column
  column <- 2
  while (column-1 <= numberOfColumnsToIterate){
    
    y<-dataset[,column]
    y
    index <- (column-1)
    currentColor <- colorScheme[index];
    #lines(x,y,col=currentColor)
    points(x,y,pch=4,col=currentColor)
    column <- column + 1
  }
  
  dev.off()
  return(1)
}
