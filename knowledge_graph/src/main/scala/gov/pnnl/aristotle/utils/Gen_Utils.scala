package gov.pnnl.aristotle.utils


import scala.io.Source
import scala.xml.XML
import util.control.Breaks._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.collection.immutable.Vector
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import org.apache.commons.lang3.StringUtils._

object Gen_Utils{
  
  def main(args: Array[String]) = {
    println(stringSim("Ford Field".toLowerCase(), "edsel_ford"))
  }
  def time[R](block: => R, function_desc : String ): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + function_desc + " =" + (t1 - t0)*1e-9 + "s")
    result
  }
  
   def writeToFile(filename: String, data: String) ={   Files.write(Paths.get(filename), data.getBytes(StandardCharsets.UTF_8)) }
  
   /* Find similarity as presence of queryPhrase in the databasePhrase
    * For e.g  if 
    * score(query = "ABC", database = "ABC DEF") = 1.0 ,
    * i.e.t the query phrase completely occurred in database phrase
    * */
   def stringSim(queryPhrase: String, databasePhrase: String, matchThreshold: Double =0.7): Double ={
     
    val splitters : Array[Char] = Array(' ', '_', ',', '$')
    // get a score for Partial match
    val wordsInPhrase1 : Array[String] = queryPhrase.toLowerCase().split(splitters).sorted
    val numWords1 = wordsInPhrase1.length
    //wordsInPhrase1.foreach(println(_))
    //println()
    val wordsInPhrase2 : Array[String] = databasePhrase.toLowerCase().split(splitters).sorted
    val numWords2 = wordsInPhrase2.length
    //wordsInPhrase2.foreach(println(_))
    //println()
   
    val setSimilarity: Double = (wordsInPhrase1.intersect(wordsInPhrase2).size).toDouble/(numWords1)
    if(setSimilarity > matchThreshold) {
      //println("Matched following phrases (score)=", queryPhrase, databasePhrase, setSimilarity)
      return setSimilarity 
   }
    
    var i: Int = 0
    var j: Int = 0
    var score: Double = 0.0
    while(i < numWords1 && j < numWords2){
      val word1 = wordsInPhrase1(i)
      val word2 = wordsInPhrase2(j)
      
      val d: Double = getLevenshteinDistance(word1.toLowerCase().toCharArray(), word2.toLowerCase.toCharArray()).toDouble
      
      val wordScore: Double = (1 - (d/word1.length)) + (1- (d/word2.length))
      //println("Distance between words and the score =", word1, word2, d, wordScore )
      if( wordScore >= matchThreshold*2) {
         //Words almost match, lets count it as a match
         i+=1
         j+=1
         score = score + 2.0
      } else {
         if(word1 < word2)
           i+=1
         else 
           j+=1
         //score = score + wordScore
      }
    }
    val finalScore = score/(numWords1+numWords2)
    //if(finalScore > matchThreshold)
    //  println("Matched " + queryPhrase + ";" + databasePhrase + "=" + finalScore.toString)
    finalScore
   }
  
}