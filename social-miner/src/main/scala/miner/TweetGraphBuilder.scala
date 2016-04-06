package miner

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import scala.collection.Set
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import scala.util.Random
import scala.collection.mutable.TreeSet
import scala.collection.immutable.TreeMap
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = 
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = 
    key.asInstanceOf[String]
}



object TwitterAnalyzer {
  def isValidLine(ln : String) : Boolean ={
    ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false) && ln.startsWith(")") == false)
  } 
  def time[R](block: => R, function_desc : String ): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + function_desc + " =" + (t1 - t0)*1e-9 + "s")
    result
  }
  def writeToFile(filename: String, data: String) ={   Files.write(Paths.get(filename), data.getBytes(StandardCharsets.UTF_8)) }
  /*
  def loadRDD(filename: String, isObject: Boolean, sc:SparkContext): RDD[(VertexId, String)] = {
    if(isObject) {
      val vertices: RDD[(VertexId, String)] = sc.objectFile(filename, 8)
      return vertices
    }
    else {
      val vertices: RDD[(VertexId, String)] = sc.textFile(filename, 8).filter(isValidLine(_)).map {ln => 
        val clean_ln = ln.replaceAllLiterally("(", "").replaceAllLiterally(")", "")
        val pos = clean_ln.indexOf(",<")
        val vertexid = clean_ln.substring(0, pos).toLong
        val userdata = clean_ln.substring(pos+2, clean_ln.length)
        (vertexid, userdata)
      }
      return vertices
    }
  }
*/
  def loadRDDUser(filename: String, isObject: Boolean, sc:SparkContext): RDD[(VertexId, String)] = {
    if(isObject) {
      val vertices: RDD[(VertexId, String)] = sc.objectFile(filename, 8)
      return vertices
    }
    else {
      val vertices: RDD[(VertexId, String)] = sc.textFile(filename, 8).filter(isValidLine(_)).map {ln => 
        val clean_ln = ln.replaceAllLiterally("(", "").replaceAllLiterally(")", "")
        val pos = clean_ln.indexOf(",<")
        val vertexid = clean_ln.substring(0, pos).toLong
        val userdata = clean_ln.substring(pos+1, clean_ln.length)
        (vertexid, userdata)
      }
      println(" No of users to keyword map", vertices.count)
      return vertices
    }
  }
  
    def loadRDDShingle(filename: String, isObject: Boolean, sc:SparkContext): RDD[(VertexId, String)] = {
    if(isObject) {
      val vertices: RDD[(VertexId, String)] = sc.objectFile(filename, 8)
      return vertices
    }
    else {
      val vertices: RDD[(VertexId, String)] = sc.textFile(filename, 8).filter(isValidLine(_)).map {ln => 
        val len = ln.length()
        val clean_ln = ln.substring(1, len-1)
        val pos = clean_ln.indexOf(",(")
        val vertexid = clean_ln.substring(0, pos).toLong
        val userdata = clean_ln.substring(pos+2, clean_ln.length-1)
        (vertexid, userdata)
      }
      println(" No of shingle to user map", vertices.count)
      return vertices
    }
  }
    
  def getShingleToUserMaps(userToWordsFile: String, shingleSize: Int, numShingles :Int, isObjectRDD: Boolean, outfile: String, outfile2: String, sc:SparkContext): Unit = {
    // Read from file ( userid, list fo words)
    val userLabelsWithWords: RDD[(VertexId, String)] = time(loadRDDUser(userToWordsFile, isObjectRDD, sc), "Time to load user RDD=")
    //userLabelsWithWords.foreach(println(_))

    val numRandoms = numShingles * 2
     val P = 104395301;
    val randoms = Seq.fill(numRandoms)(Random.nextInt).map(v => v % P)
    // for each (user,word list) create 'c' set of shingles, 
    val start_time = System.nanoTime()

     val userShinglesTemp: RDD[(Int, (String, Int))] = userLabelsWithWords.flatMap {v=> 
      val pos = v._2.indexOf(">,")
      val key = v._2.substring(0, pos+1)
      val values = v._2.substring(pos+2, v._2.length)
      val shingles: Set[(Int, (String, String))] = Shingles.getShingles(key, values, shingleSize, numShingles, randoms, ",")
      //shingles
      shingles.map(v => (v._1, (v._2._1, 1)))
    }


    /*val userShingles = userShinglesTemp.reduceByKey((v1, v2) => (v1._1, v1._2 + ";;" + v2._2)).mapValues(v=> (v, v._2.split(";;").length))
     val topKshingles =  userShingles.top(15)(new Ordering[(Int, ((String, String), Int))]() {
      override def compare(x: (Int, ((String, String), Int)), y: (Int, ((String, String), Int))): Int = 
      Ordering[Int].compare(x._2._2, y._2._2)
    })
    * 
    */
    val userShingles = userShinglesTemp.reduceByKey((v1, v2) => (v1._1, v1._2 + v2._2))
    val topKshingles =  userShingles.top(15)(new Ordering[(Int, (String, Int))]() {
      override def compare(x: (Int, (String, Int)), y: (Int, (String, Int)) ): Int = 
      Ordering[Int].compare(x._2._2, y._2._2)
    }) 
    
 /*   val topShinglesForMonth1= "#MH17".toLowerCase()
    val topShinglesForMonth2 = "Ukraine".toLowerCase()
    val userShingles = userShingles1.filter{ v => 
      val keyword= v._2._1.toLowerCase()
      (keyword.contains(topShinglesForMonth1) || keyword.contains(topShinglesForMonth1))
    }
    * *
    */
    val end_time = System.nanoTime()
    println("\n No of Unique Shingles= " + userShingles.count + "\nTime to create shingles (s)=", (end_time-start_time)*1e-9)
    var topKString = ""
      for(topShingle <- topKshingles) {
        topKString = topKString + topShingle._1.toString + "," + topShingle._2._1 +"," + topShingle._2._2  + "\n"
    }
    println(" Top K popular shingles")
    println(topKString)
    writeToFile(outfile2, topKString)
    //userShingles.foreach(println(_))
    userShingles.saveAsTextFile(outfile)
    val save_time = System.nanoTime()
    println("Time to save shingles to disk (s)=", (save_time-end_time)*1e-9)
  }
 /*  
  def getUserToShingleClusters(shingleToUsersFile: String, shingleSize: Int, numShingles :Int, isObjectRDD: Boolean, outfile: String, outfile2: String, sc:SparkContext): Unit = {
    // Read from file ( userid, list fo words)
    val shingleLabelsWithUsers: RDD[(VertexId, String)] = time(loadRDDShingle(shingleToUsersFile, isObjectRDD, sc), "Time to load shingle RDD=")

    //shingleLabelsWithUsers.foreach(println(_))
    val numRandoms = numShingles * 2
    val P = 104395301;
    val randoms = Seq.fill(numRandoms)(Random.nextInt).map(v => v % P)
    // for each (user,word list) create 'c' set of shingles, 
    val start_time = System.nanoTime()
     val userShingleClustersTemp: RDD[(Int, (String, String))] = shingleLabelsWithUsers.flatMap {v=>
       val pos = v._2.indexOf(",<")
      val key = v._2.substring(0, pos)
      val values = v._2.substring(pos+1, v._2.length)
      val shingles: Set[(Int, (String, String))] = Shingles.getShingles(key, values, shingleSize, numShingles, randoms,";;")
      shingles
    }

    val userShingleClusters: RDD[(Int, ((String, String), Int))] = userShingleClustersTemp.reduceByKey((v1, v2) => (v1._1, v1._2 + ";;" + v2._2)).mapValues(v=> (v, v._2.split(";;").length))
    val topKshingles =  userShingleClusters.top(15)(new Ordering[(Int, ((String, String), Int))]() {
      override def compare(x: (Int, ((String, String), Int)), y: (Int, ((String, String), Int))): Int = 
      Ordering[Int].compare(x._2._2, y._2._2)
    }) 

    
    val end_time = System.nanoTime()
    println("No of Unique Shingles= " + userShingleClusters.count + "\nTime to create shingles (s)=", (end_time-start_time)*1e-9)
    
    var topKString = ""
      for(topShingle <- topKshingles) {
        topKString = topKString + topShingle._1.toString + "," + topShingle._2._1._1 +"," + topShingle._2._1._2 + "," + topShingle._2._2.toString + "\n"
      }
    println(" Top K popular shingles")
    println(topKString)
    writeToFile(outfile2, topKString)
    userShingleClusters.saveAsTextFile(outfile)
    val save_time = System.nanoTime()
    println("Time to save shingles to disk (s)=", (save_time-end_time)*1e-9)
  }

    def getShingleMaps(inputFile: String, shingleSize: Int, numShingles :Int, isObjectRDD: Boolean, outfile: String, sc:SparkContext): Unit = {
    // Read from file ( userid, list fo words)
    val keyWithValues: RDD[(VertexId, String)] = loadRDD(inputFile, isObjectRDD, sc)
    //userLabelsWithWords.foreach(println(_))
    println(" No of users", keyWithValues.count)
    val numRandoms = numShingles * 2
     val P = 104395301;
    val randoms = Seq.fill(numRandoms)(Random.nextInt).map(v => v % P)
    // for each (user,word list) create 'c' set of shingles, 
     val shinglesTemp: RDD[(Int, (String, String))] = keyWithValues.flatMap {v=> 
      val shingles: Array[(Int, (String, String))] = Shingles.getShingles(v._2, shingleSize, numShingles, randoms, ">,", ",")
      shingles
    }


    val shingles = shinglesTemp.reduceByKey((v1, v2) => (v1._1, v1._2 + ";;" + v2._2))
    println("\n No of Unique Shingles= " + shingles.count + " FInding top 10 shingles")
    shingles.cache
    
    val formattedShingles: RDD[(Int, String)] = shingles.mapValues(v => "<" + v._1 + ">," + v._2)
    val topKshingles =  formattedShingles.mapValues(v=> (v, v.split('>').apply(1).split(";;").length)).top(10)(new Ordering[(Int, (String, Int))]() {
      override def compare(x: (Int, (String, Int)), y: (Int, (String, Int))): Int = 
      Ordering[Int].compare(x._2._2, y._2._2)
    }) 
    println(" Top K popular shingles")
    topKshingles.foreach(println(_))
    
    println("Saving shigles")
    formattedShingles.foreach(println(_))
    formattedShingles.saveAsTextFile(outfile)
  }
*/

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SocialMiner").setMaster("local"))
	Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    if(args.length < 3) {
	  println("Usage <twitter data path> <output file> <operation:json/triples> <Is Triple Graph in Object format>)")
	  println("Use 'json' option -> To read twitter in json format" )
	  println("Outputs Triples : userid, hasTweet, tweetid")
	  println("tweetid, hasTag/hasKey/hasSenti/hasLang, value")
	  println("Use 'triples' option -> To read in csv triples(default) (use 4th option = 'true' to enable read in object format)")
      println("Generates following line per user")
	  println("nodeid, <user_name>, list of keywords and hashtags")
	  exit
	}
	val filename: String = args(0)
	val outfile : String = args(1)
	val format : String = args(2)
	var objectFormat = false
	if(args.length == 4 && args(3) == "true")
	  objectFormat = true
    if(format == "triples") {
      TwitterGraph.TriplesToUserMap(filename, outfile, objectFormat, sc)
	} else if (format == "json"){	  
	  JsonToTriples.getTriples(filename, outfile, sc)
	} else if (format == "usermap" || format == "shinglemap") {
	  if(args.length < 6) {
	    println("Usage for shingling userprofiles")
	    println("<infile> <outfile> <usermap/shinglemap> <outfileTopK>  <NumShingles> <SizeOfShingle> <isObjectRDD(optional)>")
	    exit
	  }
	  val topKFile = args(3)
	  val numShingles = args(4).toInt
	  val shingleSize = args(5).toInt
	  println(numShingles, shingleSize)
	  //getShingleMaps(filename, shingleSize , numShingles, objectFormat,outfile, sc)
	  if(format == "usermap")
	    time(getShingleToUserMaps(filename, shingleSize , numShingles, objectFormat, outfile, topKFile, sc), " Time to process user profiles and create topics to user list")
	  else {
	    println("shingle clusterring disabled")
	    //time(getUserToShingleClusters(filename, shingleSize , numShingles, objectFormat, outfile, topKFile, sc), "Time to cluter topics and find user to topics list")
	  }
	}

  }
}
