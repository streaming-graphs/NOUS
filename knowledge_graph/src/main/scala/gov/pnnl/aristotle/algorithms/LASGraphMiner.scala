/**
 *
 * @author puro755
 * @dMay 1, 2016
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
object LASGraphMiner {
 
  val sparkConf = new SparkConf().setAppName("NOUS Graph Pattern Miner").setMaster("local")
    .set("spark.rdd.compress", "true").set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")

  sparkConf.registerKryoClasses(Array.empty)
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {

    val topk = 5
    val fc = classOf[TextInputFormat]
    val kc = classOf[LongWritable]
    val vc = classOf[Text]
    val text = sc.newAPIHadoopFile(args(0), fc, kc, vc, sc.hadoopConfiguration)
    val hdfspath = "hdfs:///user/spark/LASEntityPatternOP/"
    val linesWithFileNames = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        val file = inputSplit.asInstanceOf[FileSplit]
        iterator.map(tup => {
          val path_arr = file.getPath.toString().split("/")
          val day = path_arr(path_arr.length - 2)
          val fields = ReadHugeGraph.getFieldsFromPatternLine(tup._2.toString())
          (fields(0), List((day, fields(1).toLong)))
        })
      })

    val result = linesWithFileNames.reduceByKey((a, b) => a ::: b)
    //result.foreach(println)
    val interesting_pattern_entity = Array("amazon", "dji", "skywalker", "parrot", "3dr", "sale", "release", "accident", "manufactur",
      "popular", "emerging", "drone", "attack", "collision","usa","ground control","countries","popular drone")
    interesting_pattern_entity.foreach(ipe => {
      val interesting_pattern : RDD[(String,List[(String,Long)])]= result.filter(r => r._1.contains(ipe))
      val top_interesting_pattern = topK(interesting_pattern,topk)
      top_interesting_pattern.saveAsTextFile(hdfspath + ipe)
      val ipe_json_text = LASPatternJSONBuilder.getMakeJSONRDD(ipe,top_interesting_pattern,4)
      val ipe_json_file = new PrintWriter(new File(ipe+".json"))
      ipe_json_file.print(ipe_json_text)
      ipe_json_file.flush()
      println("******printing file*****")
    })
    
    for(i <- 0 to interesting_pattern_entity.length -2)
    {
      val newipe = interesting_pattern_entity(i) + "_"+interesting_pattern_entity(i+1)
      val interesting_pattern : RDD[(String,List[(String,Long)])]= result.filter(r => 
        (r._1.contains(interesting_pattern_entity(i)) && r._1.contains(interesting_pattern_entity(i+1)) ))
      val top_interesting_pattern = topK(interesting_pattern,topk)
      top_interesting_pattern.saveAsTextFile(hdfspath + newipe)
      val ipe_json_text = LASPatternJSONBuilder.getMakeJSONRDD(newipe,top_interesting_pattern,5)
      val ipe_json_file = new PrintWriter(new File(newipe+".json"))
      ipe_json_file.print(ipe_json_text)
      ipe_json_file.flush()
      println("******printing file*****")
    }

  }

  def topK(interesting_pattern : RDD[(String,List[(String,Long)])], K : Int)
  :RDD[(String,List[(String,Long)])]=
  {
    //return interesting_pattern.map(p=>(p._1,p._2.sortBy(_._2).take(K))
    val top_patterns = interesting_pattern.map(p=>(p._1, p._2.map(el 
        => el._2).reduce((el1,el2) => el1+el2))).sortBy(_._2, false).top(K)
    val keys : Array[String] =  top_patterns.map(p=>p._1)
    println("******keys size******" + keys.length)
    return interesting_pattern.filter(p=>keys.contains(p._1))
     
  }
}