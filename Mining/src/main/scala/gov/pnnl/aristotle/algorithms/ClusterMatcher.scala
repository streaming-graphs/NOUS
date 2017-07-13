/**
 *
 * @author puro755
 * @dJul 7, 2017
 * @Mining
 */
package gov.pnnl.aristotle.algorithms

import org.ini4j.Wini
import java.io.File
import org.joda.time.format.DateTimeFormat
import scala.io.Source
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.VertexRDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.JavaConversions._
import java.util.regex.Pattern
import org.apache.spark.SparkContext

/**
 * @author puro755
 *
 */
object ClusterMatcher {

  def main(args: Array[String]): Unit = {
    
     val sc = SparkContextInitializer.sc
    type DataGraph = Graph[Int, KGEdgeInt]
    type NbrTypes = Set[Int]
    /*
     * Read configuration parameters.
     * Please change the parameter in the conf file 'args(0)'. sample file is conf/knowledge_graph.conf
     */
    val confFilePath = args(0)
    val ini = new Wini(new File(confFilePath));
    val pathOfPartGraph = ini.get("run", "batchInfoFilePath");
    val pathOfDictionaryFile = ini.get("run", "pathOfDictionaryFile")
    val pathOfDescriptionFile = ini.get("run", "pathOfDescriptionFile")
    val startTime = ini.get("run", "startTime").toInt
    val batchSizeInTime = ini.get("run", "batchSizeInTime")
    val typePred = ini.get("run", "typeEdge").toInt
    val dateTimeFormatPattern = ini.get("run", "dateTimeFormatPattern")
    
    val fosDescClusterDir = ini.get("output", "fosDescClusterDir")

    type EntityCluster = (Int, Int)
    val allEntityClusterRDD: RDD[EntityCluster] =
      sc.textFile(pathOfPartGraph).filter(ln => ReadHugeGraph.isValidLineFromGraphFile(ln)).map(line =>
        {
          val cleanedLineArray = line.trim().replaceAll("\\)", "").replaceAll("\\(", "").split(",")
          (cleanedLineArray(0).toInt, cleanedLineArray(1).toInt)

        })

        // ABCDEF  12345
    type EntityDictionary = (String, Int)    
    val allEntityDictionary  : RDD[EntityDictionary] = 
    sc.textFile(pathOfDictionaryFile).filter(ln => ReadHugeGraph.isValidLineFromGraphFile(ln)).map(line =>
        {
          //177942 09B4F1FA
          val cleanedLineArray = line.trim().split(" ")
          (cleanedLineArray(1), cleanedLineArray(0).toInt)
        })
    
        // ABCDEF DataMining
        type EntityDescription = (String, String)    
    val allEntityDescriptionRDD  : RDD[EntityDescription] = 
    sc.textFile(pathOfDescriptionFile).filter(ln => ReadHugeGraph.isValidLineFromGraphFile(ln)).map(line =>
        {
          val cleanedLineArray = line.trim().split("\t")
          (cleanedLineArray(0),cleanedLineArray(1))
        })
    
        // (ABCDEF (12345 DataMining))
        val entityDictionaryDescription = allEntityDictionary.join(allEntityDescriptionRDD)
        val entityDictionaryDescriptionWithKey = entityDictionaryDescription.map(entry => (entry._2._1, (entry._1, entry._2._2)))
        //(12345 (ABCDEF DataMining))
        
        //(12345 (3 (ABCDEF DataMining))) //12345 is in 3rd clusterID
     val allEntityClusterDictionary = allEntityClusterRDD.join(entityDictionaryDescriptionWithKey)
     
     //(3 DataMining)
     val allEntityStringCluster = allEntityClusterDictionary.map(entry => (entry._2._1, entry._2._2._2))
         
val processedallEntityStringCluster = allEntityStringCluster.map(f
         =>(f._1, Set(f._2))).reduceByKey((fos1,fos2) => fos1 ++ fos2)
         
     processedallEntityStringCluster.groupByKey.map(entry 
         => entry._1 + "\t" + entry._2.toString.replaceAll("Set", "")).saveAsTextFile(fosDescClusterDir)
    }
    

}