/**
 *
 * @author puro755
 * @dMay 9, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{NewHadoopRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File

/**
 * @author puro755
 *
 */
object LASPathSearchJSONBuilder {

  val sparkConf = new SparkConf().setAppName("NOUS Graph Pattern Miner").setMaster("local")
    .set("spark.rdd.compress", "true").set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")

  sparkConf.registerKryoClasses(Array.empty)
  val sc = new SparkContext(sparkConf)
  
  def main(args: Array[String]): Unit = {
    
        val fc = classOf[TextInputFormat]
    val kc = classOf[LongWritable]
    val vc = classOf[Text]
    val text = sc.newAPIHadoopFile(args(0),
        fc, kc, vc, sc.hadoopConfiguration)
    //val hdfspath = "hdfs:///user/spark/LASPathSearchOPMay12/"
    val linesWithFileNames = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        val file = inputSplit.asInstanceOf[FileSplit]
        iterator.map(tup => {
          val path_arr = file.getPath.toString().split("/")
          val filename : String = path_arr(path_arr.length - 1)
          (filename, List(tup._2.toString()))
        })
      })

    val result : RDD[(String,List[String])]= linesWithFileNames.reduceByKey((a, b) => a ::: b)
    //result.saveAsTextFile(hdfspath)
    result.collect.foreach(a_search_answer => {
      val newipe = a_search_answer._1
      val ipe_json_text = LASPatternJSONBuilder.getMakeJSONSearchRDD(a_search_answer)
      val ipe_json_file = new PrintWriter(new File(newipe+".json"))
      ipe_json_file.print(ipe_json_text)
      ipe_json_file.flush()
      println("******printing file***** "+newipe)
      
    })
    
    
  }

}