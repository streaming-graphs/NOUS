package gov.pnnl.aristotle.algorithms.PathSearch

import scala.io.Source
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import scala.math.Ordering
import scala.util.Sorting
import java.io._
import gov.pnnl.aristotle.utils._
import gov.pnnl.aristotle.algorithms.PathSearch._


object PathSearchUtils {
  
   def getBestStringMatch[VD, VD2](labelSrc: String, labelDest: String, g : Graph[ExtendedVD[VD, VD2], String] ): 
   Array[(VertexId, ExtendedVD[VD, VD2])] = {
     
     val candidates : VertexRDD[ExtendedVD[VD, VD2]] = g.vertices.filter(v => 
       v._2.labelToString.toLowerCase().startsWith(labelSrc.toLowerCase()) 
         || v._2.labelToString.toLowerCase().startsWith(labelDest.toLowerCase()))
     var srcCandidates: Array[(VertexId, ExtendedVD[VD, VD2])] = candidates.filter(v => 
       v._2.labelToString.toLowerCase().startsWith(labelSrc.toLowerCase())).collect
     var destCandidates: Array[(VertexId, ExtendedVD[VD, VD2])] = candidates.filter(v => 
       v._2.labelToString.toLowerCase().startsWith(labelDest.toLowerCase())).collect
     if(srcCandidates.length < 1 || destCandidates.length < 1){
       println("No Match found for the provided labels #(src, dest) matches", 
           labelSrc, srcCandidates.length, labelDest, destCandidates.length)
       return Array.empty[(VertexId, ExtendedVD[VD, VD2])]
     }
    
     Sorting.quickSort(srcCandidates)(Ordering.by[(VertexId, ExtendedVD[VD, VD2]), String](_._2.labelToString))
     Sorting.quickSort(destCandidates)(Ordering.by[(VertexId, ExtendedVD[VD, VD2]), String](_._2.labelToString))
 
     return Array(srcCandidates(0), destCandidates(0))
   }

   def getPopularMatch[VD, VD2](label: String, g : Graph[ExtendedVD[String, Int], String]): 
   Array[(VertexId, ExtendedVD[String, Int])] = {
     
     var candidates = g.vertices.filter(v => 
       v._2.labelToString.toLowerCase().startsWith(label.toLowerCase()))
     
     //Get matching source     
     if(candidates.count == 0) {
       println("No match found that starts with label,(looking for apporoximate match) ", label)
       candidates = g.vertices.filter(v => v._2.labelToString.toLowerCase().
           contains(label.toLowerCase()))
       if(candidates.count == 0) {
          candidates = g.vertices.filter(v => Gen_Utils.stringSim(
              label.toLowerCase(), v._2.labelToString) > 0.7)
          if (candidates.count == 0)
                return Array.empty[(VertexId, ExtendedVD[String, Int])]
       }
     }
     var bestMatchingCandidates: Array[(VertexId, ExtendedVD[String, Int])] = 
       candidates.collect 
     Sorting.quickSort(bestMatchingCandidates)(
         Ordering.by[(VertexId, ExtendedVD[String, Int]), Int](_._2.extension.get))
    
     //bestMatchingCandidates.foreach(node => println(node._1, node._2.label, node._2.extension))
     return Array(bestMatchingCandidates.last)
   }
   
   /* Print Paths as following:
    * Each edge on separate line
    * Each path separated by multiple(2) lines
    */
   def printAndWritePaths(allPaths : List[List[PathEdge]], outFile: String): Unit = {
     if(allPaths.length > 0) {
          val header = "Number of paths = " + allPaths.length + "\n"
          allPaths.foreach(path => path.foreach{ edge => 
            println(edge.edgeToString) 
            println
          })
          val allPathString: String = allPaths.map(path => 
            path.map(edge => edge.edgeToString).
            reduce((edge1String, edge2String) => edge1String + "\n" + edge2String)).
            reduce((path1String, path2String) => path1String + "\n\n" + path2String)
          Gen_Utils.writeToFile(outFile, header + allPathString + "\n")
        }
   }
  

    
   /*
   def printAndWritePathsAlias[VD, ED](allPaths: List[List[PathEdge]], 
      filename: String): Unit = {
    if(allPaths.length > 0) {
      println(" Paths = \n")
      allPaths.foreach(path => path.foreach(edge => println(edge.edgeToString)))
      println("writing to file")
      val pathString: String = allPaths.map(path =>
         path.map(pathedge => 
           { 
             val srcLabel = dropAlias(pathedge.srcLabel)
             val destLabel = dropAlias(pathedge.dstLabel)
              if(pathedge.isOutgoing){
                srcLabel + "\t" + pathedge.edgeLabel + "\t" + destLabel
              } else {
                destLabel + "\t" + pathedge.edgeLabel + "\t" + srcLabel
              }
            }).
            reduce((edge1String, edge2String) => edge1String + "\n" + edge2String)).
            reduce((path1String, path2String) => path1String + "\n\n" + path2String)
       
       Gen_Utils.writeToFile(filename, pathString)
     }
    
  }
    
  def dropAlias(labelWithAlias: String): String = {
    if(labelWithAlias.contains("ALIAS")) {
          val pos = labelWithAlias.find("ALIAS")
          val label = labelWithAlias.dropRight(labelWithAlias.length() - pos)
          label
    } else 
      labelWithAlias
  }
  */
   
  
   def GetEntityPairs(filename: String, sc: SparkContext): List[(String, String)] = {
     val entityPairs = sc.textFile(filename).filter(isValidLine(_)).map(v=> v.split("\t")).filter(_.size == 2).map(v => (v(0), v(1)))
     entityPairs.foreach(println(_))
     entityPairs.collect.toList
   }
   
   def isValidLine(ln : String) : Boolean ={
    ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false))
   }
   
    /* 
   def getFilterType(filterFuncArgs : Array[String]): String = {
     val len = filterFuncArgs.length
     match args(0).toLowerCase {
         case "degree" =>  "degree"
         case "topic" => "topic"
         case _ => "None"
     }
   }
   * 
   */
}