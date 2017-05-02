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
    val filename = "edgeLabelDistribution2Full"
    //val filename = "typenode/part-00000"
    //val filename = "demofile"
    //Read part file
    val validStringRDD : RDD[String] = sc.textFile(filename).filter(ln => {
      val lnarray = ln.split(",")
      val len = lnarray.length
      if(lnarray(len-1).replaceAll("\\)", "").toInt > 3)
        true
        else
         false
    })
    val validStringRDDFormatted = validStringRDD.map(ln=>{
      val lnarray = ln.split(",")
      val len = lnarray.length
      val frq = lnarray(len-1).replaceAll("\\)", "").toInt
      (ln.replaceAll(","+frq+"\\)", ")"),frq)
    })
    
    validStringRDDFormatted.map(f=>println(f._1.replaceAll("\\(List", "").replaceAll("\\)\\)", ")")+"\t"+f._2)).saveAsTextFile("FormattedTopSign")
  }

}