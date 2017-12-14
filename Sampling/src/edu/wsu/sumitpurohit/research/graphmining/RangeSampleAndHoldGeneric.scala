/**
 *
 * @author puro755
 * @dAug 5, 2017
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexRDD
import java.io.PrintWriter
import java.io.File
import scala.io.Source
import scala.util.Random
import org.apache.spark.graphx.PartitionStrategy
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import org.apache.spark.graphx.EdgeTriplet
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge

/**
 * @author puro755
 *
 */
object RangeSampleAndHoldGeneric {

    val sparkConf = new SparkConf().
    setAppName("Load Huge Graph Main").
    set("spark.rdd.compress", "true").setMaster("local")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val sc = new SparkContext(sparkConf)
    type DataGraph = Graph[Int, KGEdgeInt]
  def main(args: Array[String]): Unit = {

  
    
    //val inputfile = "/sumitData/work/myprojects/AIM/GraphMining/wsj/citeseer.lg";
    val inputfile = "/sumitData/work/myprojects/AIM/branch_master/Mining/full2010VLDBSIGKDDCIKMData.txt";
    //val outdir = "/sumitData/work/PhD/GraphSampling/Datasets/Citeseer/Input/RangeAndSampleHoldGenericAlgov1/PageRank/"
    //val outdir = "/sumitData/work/PhD/GraphSampling/Datasets/Citeseer/Input/RangeAndSampleHoldGenericAlgov4/PageRank/"
    val outdir = "/sumitData/work/PhD/GraphSampling/Datasets/2010VLDBKDDCIKM/Input/RangeAndSampleHoldGenericAlgov1/NodeRankDegree/"
//    val multi_edge_graph : Graph[String,KGEdge] = 
//      ReadHugeGraph.getGraphLG_Temporal(inputfile, sc).subgraph(epred 
//          => !epred.attr.equals("rdf:type")).partitionBy(PartitionStrategy.RandomVertexCut)
//    
    val incomingDataGraph: DataGraph = ReadHugeGraph.getTemporalGraphInt(inputfile, sc, 31556952000L).cache 
//    vSortedInfo : List[(org.apache.spark.graphx.VertexId, Double)] 
//    			= getVSortedInfo(multi_edge_graph)
    
    
    //val vInfo = incomingDataGraph.pageRank(0.0001).vertices.collect.toList
    //val vInfo = incomingDataGraph.subgraph(epred => epred.srcId < epred.dstId).triangleCount().vertices.map(v=>(v._1,v._2.toDouble)).collect.toList
    //val vInfo = incomingDataGraph.triplets.map(t
     // =>(t.srcId, Set(t.dstId))).reduceByKey((set1, set2) => set1++set2).map(f=>(f._1, f._2.size.toDouble)).collect.toList
    val vInfo = incomingDataGraph.degrees.map(f=>(f._1, f._2.toDouble)).collect.toList
    doSamplingV1(vInfo,incomingDataGraph,inputfile,outdir)
    //doSamplingV2(vSortedInfo,multi_edge_graph,inputfile,outdir)
		//doSamplingV3(vSortedInfo,multi_edge_graph,inputfile,outdir)
		//doSamplingV4_EdgeFilter(multi_edge_graph,inputfile, outdir)
  }
def doSamplingV4_EdgeFilter(multi_edge_graph : Graph[String,KGEdge], inputfile : String, outdir : String ){

    val pageRankV = multi_edge_graph.pageRank(0.0001).vertices

    val pageRankGraph = multi_edge_graph.outerJoinVertices(pageRankV) { (id, oldAttr, pageRank) =>
      pageRank match {
        case Some(pageRank) => (oldAttr, pageRank)
        case None => (oldAttr, 0.0) // No outDegree means zero outDegree
      }
    }
    val prDist = pageRankV.map(v => v._2)

    val isSimilarEdge: (EdgeTriplet[(String, Double), KGEdge], Double) => Boolean = (triple, diff) =>
      {
        Math.abs(triple.srcAttr._2 - triple.dstAttr._2) < diff
      }

    val count = prDist.count
    val mean = prDist.sum / count
    val devs = prDist.map(score => (score - mean) * (score - mean))
    val stddev = Math.sqrt(devs.sum / (count - 1))

    val range_array = Array(stddev, stddev / 2) //
    val q_arr = Array(.2, .4, .6, .8, 1.0) //
    val p_arr = Array(.2, .4, .6, .8, 1.0) //

    for (range <- range_array) {
      for (q <- q_arr) {
        for (p <- p_arr) {

          var cnt = 0;
          val requiredInitialLines: Int = 3313
          val writerSample = new PrintWriter(new File(outdir + "citeseer_sample_page_algov4_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
          for (line <- Source.fromFile(inputfile).getLines()) {
            { //breakable not required now
              cnt = cnt + 1

              if (cnt <= requiredInitialLines) {
                writerSample.println(line)
              } else { ; }
            }
          }

          pageRankGraph.triplets.collect.foreach(f => {
            if (!f.attr.getlabel.equals("rdf:type")) {

              if (!isSimilarEdge(f, range)) {
                if (Random.nextDouble <= q)
                  writerSample.println("e " + f.srcAttr._1 + " " + f.dstAttr._1 + " " + Random.nextDouble * 100)
                else if ((Random.nextDouble <= p))
                  writerSample.println("e " + f.srcAttr._1 + " " + f.dstAttr._1 + " " + Random.nextDouble * 100)
              } else
                writerSample.println("e " + f.srcAttr._1 + " " + f.dstAttr._1 + " " + Random.nextDouble * 100)

            }
          })
          writerSample.flush()
        }
      }
    }

  }

    def doSamplingV1(vSortedInfo : List[(org.apache.spark.graphx.VertexId, Double)],
      multi_edge_graph : DataGraph,inputfile : String, outdir : String )
  {
      //val typeEdge = "rdf:type"
        val typeEdge = "10"

    val range_array = Array(10, 20, 30, 40, 50) //
    val q_arr = Array(.2, .4, .6, .8, 1.0) //
    val p_arr = Array(.2, .4, .6, .8, 1.0) //

    val inRangeDouble: ((org.apache.spark.graphx.VertexId, Double), Double, Double, Int) => Boolean = (vertex, min, max, localRange) =>
      {
        val unitStep: Double = (max - min) / 100
        (vertex._2 < min + unitStep * localRange) || (vertex._2 > max - unitStep * localRange)
      }

    for (range <- range_array) {
      val filteredV = getFilteredV(vSortedInfo, inRangeDouble, range)
      for (q <- q_arr) {
        for (p <- p_arr) {
          var cnt = 0;
          val requiredInitialLines: Int = 3313
          val writerSample = new PrintWriter(new File(outdir + "fullvldbetc_algov1_nodeRank_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
/*          for (line <- Source.fromFile(inputfile).getLines()) {
            { //breakable not required now
              cnt = cnt + 1

              if (cnt <= requiredInitialLines) {
                writerSample.println(line)
              } else { ; }
            }
          }*/

          multi_edge_graph.triplets.collect.foreach(f => {
            if (!f.attr.getlabel.equals(typeEdge)) {
              if ((filteredV.contains(f.srcId)) || (filteredV.contains(f.dstId))) {
                if (Random.nextDouble <= q)
                  //writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
                  writerSample.println(f.srcAttr + "\t" +f.attr.getlabel +"\t"+ f.dstAttr + "\t" + "2010/08/13T05:01:00.000")
                else if ((Random.nextDouble <= p))
                  //writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
                  writerSample.println(f.srcAttr + "\t" +f.attr.getlabel +"\t"+ f.dstAttr + "\t" + "2010/08/13T05:01:00.000")
              }
            }
            else //write the typed Edge for MAG graphs
              writerSample.println(f.srcAttr + "\t" +f.attr.getlabel +"\t"+ f.dstAttr + "\t" + f.attr.datetime)
          })
          writerSample.flush()
        }
      }
    }

  }
  
  def doSamplingV2(vSortedInfo : List[(org.apache.spark.graphx.VertexId, Double)],
      multi_edge_graph : Graph[String,KGEdge],inputfile : String, outdir : String )
  {

    val range_array = Array(10,20,30,40,50)//
    val q_arr = Array(.2,.4,.6,.8,1.0)//
    val p_arr = Array(.2,.4,.6,.8,1.0)//

    val inRangeDouble: ((org.apache.spark.graphx.VertexId, Double), Double, Double, Int) => Boolean = (vertex, min, max, localRange) =>
      {
        val unitStep: Double = (max - min) / 100
        (vertex._2 < min + unitStep * localRange) || (vertex._2 > max - unitStep * localRange)
      }
    var visitedNodes : scala.collection.mutable.Set[Long] = scala.collection.mutable.Set.empty
    for (range <- range_array) {
      val filteredV = getFilteredV(vSortedInfo, inRangeDouble, range)
      for (q <- q_arr) {
        for (p <- p_arr) {

          var cnt = 0;
          val requiredInitialLines: Int = 3313
          val writerSample = new PrintWriter(new File(outdir + "citeseer_sample_page_algov2_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
          for (line <- Source.fromFile(inputfile).getLines()) {
            { //breakable not required now
              cnt = cnt + 1

              if (cnt <= requiredInitialLines) {
                writerSample.println(line)
              } else { ; }
            }
          }

          multi_edge_graph.triplets.collect.foreach(f => {
            if (!f.attr.getlabel.equals("rdf:type")) {
              if ((filteredV.contains(f.srcId)) || (filteredV.contains(f.dstId))) {
                if (visitedNodes.contains(f.srcId) || visitedNodes.contains(f.dstId)) {
                  if (Random.nextDouble <= q)
                    writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
                  visitedNodes += (f.srcId, f.dstId)
                } else {
                  if ((Random.nextDouble <= p))
                    writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
                  visitedNodes += (f.srcId, f.dstId)
                }

              } else {
                writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
                visitedNodes += (f.srcId, f.dstId)
              }
            } 
          })

          writerSample.flush()

        }
      }
    }

  }
  
  
  def doSamplingV3(vSortedInfo : List[(org.apache.spark.graphx.VertexId, Double)],
      multi_edge_graph : Graph[String,KGEdge],inputfile : String, outdir : String )
  {

    val range_array = Array(10, 20, 30, 40, 50) //
    val q_arr = Array(.2, .4, .6, .8, 1.0) //
    val p_arr = Array(.2, .4, .6, .8, 1.0) //

    val inRangeDouble: ((org.apache.spark.graphx.VertexId, Double), Double, Double, Int) => Boolean = (vertex, min, max, localRange) =>
      {
        val unitStep: Double = (max - min) / 100
        (vertex._2 < min + unitStep * localRange) || (vertex._2 > max - unitStep * localRange)
      }

    for (range <- range_array) {
      val filteredV = getFilteredV(vSortedInfo, inRangeDouble, range)
      for (q <- q_arr) {
        for (p <- p_arr) {
          var cnt = 0;
          val requiredInitialLines: Int = 3313
          val writerSample = new PrintWriter(new File(outdir + "citeseer_sample_page_algov3_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
          for (line <- Source.fromFile(inputfile).getLines()) {
            { //breakable not required now
              cnt = cnt + 1

              if (cnt <= requiredInitialLines) {
                writerSample.println(line)
              } else { ; }
            }
          }

          multi_edge_graph.triplets.collect.foreach(f => {
            if (!f.attr.getlabel.equals("rdf:type")) {
              if ((filteredV.contains(f.srcId)) || (filteredV.contains(f.dstId))) {
                if (Random.nextDouble <= q)
                  writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
                else if ((Random.nextDouble <= p))
                  writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
              }else
                writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
            }
          })
          writerSample.flush()
        }
      }
    }

  }
  
  def getVSortedInfo(multi_edge_graph : Graph[String,KGEdge]) 
  	: List[(org.apache.spark.graphx.VertexId, Double)] = 
  {
    //1. get diversity of the node. diversity is number of unique label 
    // Get distribution of diversity
    //val diversity = multi_edge_graph.triplets.map(t
      // =>(t.srcId, Set(t.dstId))).reduceByKey((set1, set2) => set1++set2).map(f=>(f._1, f._2.size.toDouble)).collect.toList
    
    
    /*val count = divDist.count
    val divDist = diversity.map(t 
        => (t._1, t._2.size)).map(entry=>entry._2)
		val mean = divDist.sum / count
		val devs = divDist.map(score => (score - mean) * (score - mean))
		val stddev = Math.sqrt(devs.sum / (count - 1))
		
		divDist.collect.foreach(f=>println(f))
		println("SUmi is ", mean)
		println("sdt dev : in ", stddev)
		System.exit(1)
		*/

    //val error = multi_edge_graph.triplets.filter(t=>t.srcId > t.dstId)
//    println("error count is " , multi_edge_graph.triplets.count, error.count)
//    System.exit(1)
    
    //2. Get number of triangle for each vertesx
  	val sortedVTriangle  
  			= multi_edge_graph.subgraph(epred => epred.srcId < epred.dstId).triangleCount().vertices.map(v=>(v._1,v._2.toDouble)).collect.toList
    
    // 3. sorted page Rank
    val sortedVPageRank = multi_edge_graph.pageRank(0.0001).vertices.collect.toList
    
    
    
    
    return sortedVTriangle
    //return sortedVPageRank
    //return diversity
  }

  def getFilteredV(vSortedInfo: List[(org.apache.spark.graphx.VertexId, Double)],
    filterFunction: ((org.apache.spark.graphx.VertexId, Double), Double, Double, Int) => Boolean,
    range: Int): Map[Long, Double] =
    {
      val min = vSortedInfo.map(f=>f._2).min
      val max = vSortedInfo.map(f=>f._2).max
      val res = vSortedInfo.filter(p => filterFunction(p, min,max, range)).toMap
      res

    }
}