package miner

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonToTriples {
  
  def isValidLine(ln : String) : Boolean ={
    ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false) && ln.startsWith(")") == false)
  }
  def getTriples(filename: String, outfile: String, sc: SparkContext) = {
  val tweets: RDD[(String, String)] = sc.textFile(filename).filter(line =>isValidLine(line)).map(
	      v => TwitterJsonReader.getTriples(v)).partitionBy(new HashPartitioner(300)).cache
	  println("Number of tweets = " + tweets.count())
	  val groupedTweets = tweets.groupByKey()
	  println("Number of keys(datebased) = " + groupedTweets.keys.count)
	  groupedTweets.saveAsHadoopFile(outfile, classOf[String], classOf[String],
	      classOf[RDDMultipleTextOutputFormat])
 }
}

object TwitterJsonReader {
    
    def getTriples(jsonString: String): (String,String) = {
      val json: JValue = parse(jsonString)
      implicit val formats = DefaultFormats
      val userid = (json\"actor"\"preferredUsername").extract[String]//getUserId(json)
      // println("user id =" + userid.values)
    
      val tweetid_string = (json\ "id" ).extract[String]
      val pos = tweetid_string.indexOf(',')
      val tweetid = tweetid_string.substring(pos+1, tweetid_string.length).replace(':', '_')
       //println("tweet id =" + tweetid)
      val language  = (json\"twitter_lang").extract[String]
       //println("languages = " + languages.values)
      val hashtags = getHashTags(json)
      var htriples = ""
        for (hashtag <- hashtags){
          htriples += tweetid + ",hasTag," + hashtag +"\n"
        }
       //hashtags.foreach(hashtag => println("hashtags = " + hashtag))

      val keywords = getKeywords(json, hashtags.length)
      var keytriples = ""
        for (key <- keywords){
          keytriples += tweetid + ",hasKey," + key + "\n"
        }
      //keywords.foreach(kw => println("keywords = " + kw))
      val sentiments = getSentiments(json) //json\"document"\"keywords"
      var striples = ""
        for (sentiment <- sentiments){
          striples += tweetid + ",hasSenti," + sentiment + "\n"
        }
      val timestamp = (json\"postedTime").extract[String]
      var date = timestamp
      if(timestamp.length() > 10 )
        date = timestamp.substring(0,10)
      val result = userid + ",hasTweet," + tweetid + "\n" + 
    		  tweetid + ",hasLang," + language + "\n" +
    		  htriples + keytriples + striples
      	  
      //println(date,result)
      return (date,result)
    }

    def getHashTags(json: JValue): Array[String] = {
      val data = for {
          JObject(tweet) <- json
          JField("body", JString(body)) <- tweet
        } yield (body)
        if(data.length <=0 ) {
          println("there is no body/text in tweet" + json) 
          exit
        }
      val hashtags = data(0).split(" ").filter(t => t.length > 0).filter(t => t(0) == '#')
      hashtags
    }
    
    def getKeywords(json: JValue, hashtagLen: Int): List[String] = {
      implicit val formats = DefaultFormats
      if(hashtagLen > 0) 
        return List.empty
      else 
        return (json\"document"\"keywords").extract[List[String]]
    }
    
    def getSentiments(json: JValue): List[String] = {
      val sentiments = (json\"document"\"sentiment")
      var x: String = ""
      val sentiment_list = for {
        JObject(sObj) <- sentiments
        JField(x, JBool(true)) <- sObj
      } yield(x)
      sentiment_list
    }

  }
