/**
 *
 */
package gov.pnnl.aristotle.algorithms

import gov.pnnl.fcsd.datasciences.graphBuilder.nlp.wordsimilarity.WordNetSimilarity
import util.control.Breaks._
import java.io.PrintWriter
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import scalaz.Scalaz._
import scala.collection.SortedMap

/**
 * @author puro755
 *
 */
object WordNet_Mapper_Util {

   /*
   * Initialize the spark context and its run-time configurations
   */

  val sparkConf = new SparkConf().setAppName("Load Huge Graph Main")
    .setMaster("local").set("spark.rdd.compress", "true").set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")

  sparkConf.registerKryoClasses(Array.empty)
  val sc = new SparkContext(sparkConf)

  /*
   * Initialize Log File Writer & Set up the Logging level
   */
  val writerSG = new PrintWriter(new File("GraphMiningOutput2k13.txt"))
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  
  /*
   * Initialize the Object with all required global variables so that they are 
   * read only once
   */
  val word_sim = new WordNetSimilarity
  val wordnet_rdd : RDD[(String, Long)] = getWordNetRDD()

  /*
   * Read wsj verbs file RelationSynonymstestWSJ2010
   * And initialize the cannonical verb lookup DS
   */
  val canonical_verb_Map1 : RDD[(String, String)] =   getCanonicalVerbMap()
  val canonical_verb_Map : Map[String,String] = canonical_verb_Map1.map(f 
      => Map(f._1 -> f._2)).reduce((a,b) => a|+|b )
  
  def main(args: Array[String]): Unit = {

    val writer1 = new PrintWriter(new File("wsj_verb_yago_wordnetid_full.txt"))
    val writer2 = new PrintWriter(new File("wsj_verb_yago_wordnetid_top1.txt"))

    //val canonical_verb_broadcast = sc.broadcast(canonical_verb_Map.map(vline => vline._1))
    println("canonical size : " + canonical_verb_Map.size)
    /*
     * Calculate all the similarity values between every wordnet id and 
     * canonical verb. It leads to N*M computation of about 54M values
     */
    val wordnetid_verb_RDD : Map[String,Set[(Double,String)]] = 
      getCorrespondingWordNetIDs(canonical_verb_Map)
    
    
   // wordnetid_verb_RDD.collect.foreach(f => println(f.toString))
    val reduced_wordnetid_verb_map = wordnetid_verb_RDD.map(f => Map(f._1 -> f._2)).reduce((a, b) => a |+| b)
    println("done" + reduced_wordnetid_verb_map.size)

    reduced_wordnetid_verb_map.foreach(f => {
      //Write full dictionary
      val sortedMap = f._2.toSeq.sortWith(_._1 > _._1)
      writer1.print(f._1 + "=>")
      sortedMap.foreach(wid_confidence => writer1.print(wid_confidence._1 + ":" + wid_confidence._2 + "\t"))
      writer1.println()

      //Write top 1 dictionary
      writer2.println(f._1 + "=>" + sortedMap.head._1 + ":" + sortedMap.head._2)

    })
    writer1.flush()
    writer2.flush()

  }

  def getCorrespondingWordNetIDs(canonical_verb_Map : Map[String,String])
  : Map[String,Set[(Double,String)]] = 
  {
      val wordnetid_verb_RDD =
        wordnet_rdd.flatMap(wordnetid => {
          var answer: Set[(String, Set[(Double, String)])] = Set.empty

          canonical_verb_Map.foreach(canonical_verb =>
            {
              answer = answer + ((canonical_verb._1, Set((WordNetSimilarity.public_run(canonical_verb._1,
                wordnetid._1), "wordnet_" + wordnetid._1 + "_" + wordnetid._2))))
            })
          answer
        })

      val reduced_wordnetid_verb_map = wordnetid_verb_RDD.map(f => Map(f._1 -> f._2)).reduce((a, b) => a |+| b)

      return reduced_wordnetid_verb_map
    }
  
  def getCanonicalVerb(nlpVerb: String): String =
    {
      val answer  = canonical_verb_Map.filter(f => f._2.contains(nlpVerb))
      if (answer.size == 0)
        return "nous_" + nlpVerb
      else
        return answer.head._1
    }

  def getSimilarWordNetIds(nlpVerb: String, threshold : Double): RDD[(String, Double)] =
    {
      val canonical_verb = getCanonicalVerb(nlpVerb)
      var answer: RDD[(String, Double)] =
        wordnet_rdd.filter(wordnetid => WordNetSimilarity.public_run(canonical_verb,
          wordnetid._1) > threshold)
          .map(wordnetid => {
            ((wordnetid._1,
              WordNetSimilarity.public_run(canonical_verb,
                wordnetid._1)))
          })
      return answer
    }
  def getSimilarity(canonicalVerb: String): Double =
    {

      return 0.0
    }

  def getWordNetRDD() : RDD[(String,Long)] = {

    return sc.textFile("yagoWordnetIds.ttl").filter(line =>
      !(line.startsWith("@") || line.startsWith("#") || line.isEmpty())).map { line =>
      {
        val line2 = line.substring(0, line.lastIndexOf("."));
        val linearray = line2.toLowerCase().replaceAll("<", "").replaceAll(">", "").split("\\t");
        (linearray(0).replaceAll("wordnet_", "").replaceAll("[0-9]", "").replaceAll("_", " ").trim(),
                       linearray(2).replaceAll("\"", "").trim().toInt);
      }
    }
  }

  def getCanonicalVerbMap() : RDD[(String,String)] = {

    return sc.textFile("RelationSynonymstestWSJ2010.txt").filter(line =>
      !(line.startsWith("@") || line.startsWith("#") || line.isEmpty())).map { line =>
      {
        val linearray = line.toLowerCase().replaceAll("<", "").replaceAll(">", "").split(":");
        (linearray(0), linearray(1).replaceAll("\"", "").trim());
      }
    }

  }
}  
