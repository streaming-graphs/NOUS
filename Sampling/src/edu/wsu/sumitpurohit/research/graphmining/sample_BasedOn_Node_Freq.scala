package gov.pnnl.aristotle.algorithms.test

import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.PrintWriter
import java.io.File
import scala.util.Random
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD

object sample_BasedOn_Node_Freq {

    def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").
    setAppName("Load Huge Graph Main").
    set("spark.rdd.compress", "true")
    val sc = new SparkContext(sparkConf)
    
    val inputfile = "/sumitData/work/myprojects/AIM/GraphMining/wsj/citeseer.lg";
    val multi_edge_graph : Graph[String,KGEdge] = 
      ReadHugeGraph.getGraphLG_Temporal(inputfile, sc).subgraph(epred 
          => !epred.attr.equals("rdf:type"))

    val degree : VertexRDD[Int] = multi_edge_graph.degrees
    //degree.collect.foreach(f=>writerSample.println(f._1+":"+f._2))
    val number_of_edges = multi_edge_graph.edges.count
    val sorted_degree = degree.sortBy(f => f._2)
    val sorted_degree_list = sorted_degree.collect.toList
    val list_size = sorted_degree_list.size
    val range = 15
    //sorted_degree_list.foreach(f=>println(f._1 + " : " +f._2))
   
    val range_arr = Array(50)//10,20,30,40,
    val accelerator_arr = Array(50)//1,10,20,30,40,
    for(range <- range_arr)
    {
      for(accelerator <- accelerator_arr)
      {
        // Get vertices in the range
        var top_bottom_vid: Map[Long,Int] = Map.empty
        val writerSample = new PrintWriter(new File("citeseer_sample_range_" + range + "_accelerator_" + accelerator + "test.lg"))
        var i = 0;
        sorted_degree_list.foreach(f =>
          {
            if ((i < (list_size.toFloat / 100) * range) || (i > (list_size.toFloat / 100) * (100 - range))) {
              top_bottom_vid = top_bottom_vid + (f._1 -> f._2)
              writerSample.println("adding value" + f._1 +":"+f._2)
            }
            i = i + 1
          })
        writerSample.println("final i value is "+i)
        //Write some initial line in the .lg file which shows node id  
        var cnt = 0;
        val requiredInitialLines: Int = 3313
        writerSample.println("t # 1")
        for (line <- Source.fromFile(inputfile).getLines()) {
          { //breakable not required now
            cnt = cnt + 1

            if (cnt < requiredInitialLines) {
              writerSample.println(line)
            } else { ; }
          }
        }

        //write only the edge which has src or dst in the set of vertices in the range
        multi_edge_graph.triplets.collect.foreach(f => {
          if (!f.attr.getlabel.equals("rdf:type")) {
            writerSample.println("eevery nontype edge "+f.srcId+ " " +f.dstId + " "+Random.nextDouble * 100)
            // println(f)
            var prob1: Double = 0.0
            var prob2: Double = 0.0
            if ((top_bottom_vid.contains(f.srcId))) {
              // nodes are in the sample list
              writerSample.println("Source found "+f.srcId+ " " +f.dstId + " "+Random.nextDouble * 100)
              prob1 = ((top_bottom_vid.getOrElse(f.srcId, 0)).toDouble / (2 * number_of_edges)) * accelerator
            }
            if ((top_bottom_vid.contains(f.dstId))) {
              // nodes are in the sample list
              writerSample.println("Destination found "+f.srcId+ " " +f.dstId + " "+Random.nextDouble * 100)
              prob2 = ((top_bottom_vid.getOrElse(f.dstId, 0)).toDouble / (2 * number_of_edges)) * accelerator
            }
            var final_prob = prob1
            if (prob2 > prob1) final_prob = prob2
            writerSample.println("prob is " + final_prob + " " + top_bottom_vid.getOrElse(f.srcId, 0).toDouble + " :" + 
            top_bottom_vid.getOrElse(f.dstId, 0).toDouble)
            if (Random.nextDouble > final_prob)
            {
              writerSample.println("writing something " + final_prob)
              writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
            }
              
          }

        })
        writerSample.flush()
        println("done")
        println("edge cnt" + multi_edge_graph.edges.count)
        println("triples cnt" + multi_edge_graph.triplets.count)
        println("tirple ccoll cnt" + multi_edge_graph.triplets.collect.length)
      }
    }
    

    

  }
  
  
}