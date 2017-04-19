/**
 *
 * @author puro755
 * @dJun 23, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.analysis

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
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import scala.util.Random
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
/**
 * @author puro755
 *
 */
object PatternAnalyzer {

  val sparkConf = new SparkConf().setAppName("NOUS Graph Pattern Miner").setMaster("local")
    .set("spark.rdd.compress", "true").set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")

  sparkConf.registerKryoClasses(Array.empty)
  val sc = new SparkContext(sparkConf)
  
  def get_defaultPatternRDD(args: Array[String]): RDD[(String, List[(Int,Long)])] =
    {
      val fc = classOf[TextInputFormat]
      val kc = classOf[LongWritable]
      val vc = classOf[Text]
      val text = sc.newAPIHadoopFile(args(0), fc, kc, vc, sc.hadoopConfiguration)
      val default_pattern_rdd: RDD[(String, List[(Int,Long)])] 
      = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
        .mapPartitionsWithInputSplit((inputSplit, iterator) => {
          val file = inputSplit.asInstanceOf[FileSplit]
          iterator.map(tup => {
            val path_arr = file.getPath.toString().split("/")
            val day = path_arr(path_arr.length - 2)
            val fields = ReadHugeGraph.getFieldsFromPatternLine_BatchFormat(tup._2.toString())
            (fields(0)._1, fields(0)._2)
          })
        })

      return default_pattern_rdd
    }
  
  
  def get_nodePatternRDD(args: Array[String]): RDD[(String, (Int,List[String]))] =
    {
      val fc = classOf[TextInputFormat]
      val kc = classOf[LongWritable]
      val vc = classOf[Text]
      val text = sc.newAPIHadoopFile(args(1), fc, kc, vc, sc.hadoopConfiguration)
      val default_pattern_rdd: RDD[(String, (Int,List[String]))] 
      = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
        .mapPartitionsWithInputSplit((inputSplit, iterator) => {
          val file = inputSplit.asInstanceOf[FileSplit]
          iterator.map(tup => {
            val path_arr = file.getPath.toString().split("/")
            val day = path_arr(path_arr.length - 2)
            val fields = ReadHugeGraph.getFieldsFromPatternNodeLine(tup._2.toString())
            (fields(0)._1, fields(0)._2)
          })
        })

      return default_pattern_rdd
    }
  
  def main(args: Array[String]): Unit = {
    val pattern_batch_rdd = get_defaultPatternRDD(args)
    val total_patterns = pattern_batch_rdd.count
    val pattern_node_rdd = get_nodePatternRDD(args)
    val joined_rdd = pattern_batch_rdd.join(pattern_node_rdd)
    //joined_rdd.collect.foreach(f=> println(f.toString))
    val libsvm_writer = new PrintWriter(new File("GraphMining_libsvm.txt"))
    joined_rdd.collect.foreach(f => {
      var outputstr = "1 "
      val patternlist = f._2._1
      val all_support = patternlist.map(p => p._2)
      val avg = all_support.sum / all_support.length
      //label:avg_support,participating_nodes,participating_patterns,frequent_batches
      outputstr = outputstr + s"1:$avg 2:" + f._2._2._1 + " 3:" + Random.nextInt(total_patterns.toInt) + " 4:" + Random.nextInt(3)
      libsvm_writer.println(outputstr)
    })
    libsvm_writer.flush()

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "GraphMining_libsvm.txt")
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    //model.save(sc, "myModelPath")
    //val sameModel = SVMModel.load(sc, "myModelPath")
  }

}