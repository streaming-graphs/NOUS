/**
 *
 * @author puro755
 * @dApr 5, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import gov.pnnl.aristotle.algorithms.mining.datamodel.VertexProperty

/**
 * @author puro755
 *
 */
object YagoTypeTreeLoader {

  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("NOUS Graph Pattern Miner").setMaster("local")
      .set("spark.rdd.compress", "true").set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")

    sparkConf.registerKryoClasses(Array.empty)
    val sc = new SparkContext(sparkConf)
    val rdd  : RDD[(VertexId,List[VertexProperty])] = sc.objectFile("typerdd.bin")
    println("size of the rdd is" + rdd.count)
    val sample_output = rdd.filter(f=>f._2.size > 1).take(100)
    sample_output.foreach(f=>{
      println("****Node Tree**")
      f._2.foreach(f=>println(f.id+":"+f.property_label))
    })
  }

}