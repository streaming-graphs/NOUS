package miner

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.TreeMap

object Shingles {
def getShingle(key: String, valuesString: String, shingleSize: Int, random1: Int, random2: Int, valueSplitter: String) : (Int, (String, String)) ={
 
    // create a (shingle, key), using 'shingleSize' number of values  to create the shingle      
     val P = 104395301; //http://primes.utm.edu/lists/small/millions/, 104M
     
     // get(key, list(values))
      //val pos = keyToValue.indexOf(keyToMapSplitter) 
      //val key: String = keyToValue.substring(0, pos+1).trim()
      //println("in line" + keyToValue + ", User = " + key)
      //val startOfValues : Int = pos + keyToMapSplitter.length
      //val values: Array[String] = keyToValue.substring(startOfValues, keyToValue.length).split(valueSplitter).map(_.trim())
      //println("username =" + username  + ", user keywords=" + keywords.toString())
      
      // Map values to hashcodes
      val values = valuesString.split(valueSplitter).map(_.trim())
      val valuesMap: Map[Int, String] = values.map(v => ((random1*v.hashCode() + random2)%P -> v)).toMap
      //println("printing hashcode(keyword), keyword")
      //keywordMap.foreach(println(_))
      
      // Take top '#shingleSize" number of hashcodes 
      val emptyShingles = TreeMap.empty[Int, String]    
      val sortedShingles:TreeMap[Int, String] = emptyShingles ++ valuesMap
      val topShingles = sortedShingles.take(shingleSize).toList
      val reducedShingle: (String, String) = topShingles.map(v => (v._1.toString, v._2)).reduce(
          (v1, v2) => ((v1._1+ ";" + v2._1), (v1._2 + ";" + v2._2)))
      
      // create a shingle id 
      val shingleid = reducedShingle._1.hashCode()
      val shingleString = reducedShingle._2
      
      // send (shingle id, shingle representative string, key)
      //println(shingleString)
      return (shingleid, (shingleString, key))         
  }
  
  // create set of '#numShingles' shingle each of size '#shingleSize'
  // @Input : keyToValue is a string of form :
  // $key $keyToValueSplitter $value_1 $valuesSplitter $value_2 $valuesSplitter.. ... $value_N
  // Output : An array of shingles, each shingle being
 // (shingle id, shingle representative string, key)
 // NOtice 'key' is same across the set of shingles 
  def getShingles(key: String, values: String, shingleSize: Int, numShingles: Int, randoms: Seq[Int], valuesSplitter: String) : Set[(Int, (String, String))] = {    
    var allShingles = new ArrayBuffer[(Int, (String, String))](numShingles)
    for(i <- Range(0,numShingles*2, 2)){
      val shingle = getShingle(key, values, shingleSize, randoms(i), randoms(i+1), valuesSplitter)
      allShingles += shingle
    }
    return allShingles.toSet
  }
}