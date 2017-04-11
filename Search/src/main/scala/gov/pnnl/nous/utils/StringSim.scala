package gov.pnnl.nous.utils

import org.apache.commons.lang3.StringUtils._

object StringSim {
  def getsim(queryPhrase: String, databasePhrase: String, matchThreshold: Double =0.7): Double ={

    val splitters : Array[Char] = Array(' ', '_', ',', '$')
    // get a score for Partial match
    val wordsInPhrase1 : Array[String] = queryPhrase.toLowerCase().split(splitters).sorted
    val numWords1 = wordsInPhrase1.length
    val wordsInPhrase2 : Array[String] = databasePhrase.toLowerCase().split(splitters).sorted
    val numWords2 = wordsInPhrase2.length

    val setSimilarity: Double = (wordsInPhrase1.intersect(wordsInPhrase2).size*2).toDouble/(numWords1+numWords2)
    if(setSimilarity >= matchThreshold) {
      println("String matched following phrases (score)=", queryPhrase, databasePhrase, setSimilarity)
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

    //  println("Matched " + queryPhrase + ";" + databasePhrase + "=" + finalScore.toString)
    finalScore
   }

}