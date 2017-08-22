/**
 *
 * @author puro755
 * @dMay 1, 2017
 * @Mining
 */
/**
 * @author puro755
 *
 */
package gov.pnnl.aristotle.algorithms

import org.apache.spark.rdd.RDD

object parseRawOutput {

  def main(args: Array[String]): Unit = {
    
    val sc = SparkContextInitializer.sc
    //val filename = "TypeClusetIDinSignature_withClusering/part-00000"
      val filename = "TypeClusetIDinSignature_withKmeanClusering5.txt"
    //val filename = "typenode/part-00000"
    //val filename = "demofile"
    //Read part file
    /*val validStringRDD : RDD[String] = sc.textFile(filename).filter(ln => {
      val lnarray = ln.split(",")
      val len = lnarray.length
      if(lnarray(len-1).replaceAll("\\)", "").toInt > 3)
        true
        else
         false
    })
    */
    val validStringRDDFormatted = sc.textFile(filename).map(ln=>{
      val lnarray = ln.split(",")
      val len = lnarray.length
      val frq = lnarray(len-1).replaceAll("\\)", "").toInt
      (ln.replaceAll(","+frq+"\\)", ")"),frq)
    })
    validStringRDDFormatted.collect.foreach(p=>println(p))
    validStringRDDFormatted.map(f=>(f._1.replaceAll("\\(List", "").replaceAll("\\)\\)", ")")+"\t"+f._2)).saveAsTextFile("TypeClusetIDinSignature_withKmeanClusering5Parsed.txt")
  }

}