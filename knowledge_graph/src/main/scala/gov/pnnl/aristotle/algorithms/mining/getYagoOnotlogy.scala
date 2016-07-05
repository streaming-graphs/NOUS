/**
 *
 * @author puro755
 * @dMar 27, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining

import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeDirection
import gov.pnnl.aristotle.algorithms.mining.datamodel.VertexProperty
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import gov.pnnl.aristotle.algorithms.mining.datamodel.VertexProperty
import java.io.PrintWriter
import gov.pnnl.aristotle.algorithms.mining.datamodel.VertexProperty
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2Flat
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2Flat
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId

/**
 * @author puro755
 *
 */
object getYagoOnotlogy extends Serializable {

  val writerSG = new PrintWriter(new File("yagoOntologyOP.txt"))
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NOUS Graph Pattern Miner")
      .set("spark.rdd.compress", "true").set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")

    sparkConf.registerKryoClasses(Array.empty)
    val sc = new SparkContext(sparkConf)
    //val filepath = "/sumitData/work/myprojects/AIM/yagoTaxonomy.small2.ttl"
    val filepath = args(4)

    val multi_edge_graph = ReadHugeGraph.getTemporalGraph(filepath, sc)
    val gBatch = new DynamicPatternGraphV2Flat(1).init(multi_edge_graph,
      writerSG, "rdf:type", 100)
    val new_graph = gBatch.input_graph.mapVertices((id, data) => new KGNodeV2Flat(data.getlabel, data.getpattern_map, List(new VertexProperty(1, data.label))))
    val res = getYagoOnoglogyRDD(new_graph)

    res.vertices.foreach(v => {
      writerSG.println("*****New Node***** = " + v._2.getlabel)
      v._2.getProperties.foreach(p =>
        writerSG.println(" Prop " + p.id + ":" + p.property_label))

    })
    
    val type_rdd : RDD[(VertexId,List[VertexProperty])] = res.vertices.map(vertex => (vertex._1,vertex._2.getProperties))
    type_rdd.saveAsObjectFile("typerdd3.bin")
    writerSG.flush()
  }

  def getYagoOnoglogyRDD(graph: Graph[KGNodeV2Flat, KGEdge]): Graph[KGNodeV2Flat, KGEdge] =
    {
      writerSG.println("in the method")
      val newGraph = graph.pregel[List[VertexProperty]](List.empty[VertexProperty],
        3, EdgeDirection.In)(
          (id, dist, newDist) =>
            {
              new KGNodeV2Flat(dist.getlabel, dist.getpattern_map, dist.getProperties ++ newDist)
            }, // Vertex Program
          triplet => { // Send Message

            if (triplet.attr.getlabel.equals("rdfs:subclassof")) {
              val new_prop = triplet.dstAttr.getProperties.map(f => new VertexProperty(f.id << 1, f.property_label))
              Iterator((triplet.srcId, new_prop))
            } else {
              writerSG.println(" EMPTY MSG")
              Iterator.empty
            }

          },
          (a, b) => a ++ b // Merge Message
          )
      writerSG.flush()
      val udpate_grpah = newGraph.mapVertices((id, data) => {
        // keep only the minimum distance super class
        var tmp_map: Map[String, Long] = Map.empty
        data.getProperties.foreach(f => {
          if ((!tmp_map.contains(f.property_label)) ||
            (f.id < tmp_map.getOrElse(f.property_label, Long.MaxValue)))
            tmp_map = tmp_map + (f.property_label -> f.id)
        })
        var resulting_props: List[VertexProperty] = List.empty
        tmp_map.foreach(f =>
          {
            resulting_props = resulting_props ++ List(new VertexProperty(f._2, f._1))
          })

        new KGNodeV2Flat(data.getlabel, data.getpattern_map,
          resulting_props)
      })
      return udpate_grpah
    }
}  