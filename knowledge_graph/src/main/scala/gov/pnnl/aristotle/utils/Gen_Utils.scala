package gov.pnnl.aristotle.utils


import scala.io.Source
import scala.xml.XML
import util.control.Breaks._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.collection.immutable.Vector
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object Gen_Utils{
    
  def time[R](block: => R, function_desc : String ): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + function_desc + " =" + (t1 - t0)*1e-9 + "s")
    result
  }
  
   def writeToFile(filename: String, data: String) ={   Files.write(Paths.get(filename), data.getBytes(StandardCharsets.UTF_8)) }
  
   /* Implement Leavenshtein distance */
 /*  def stringSimScore(string1: String, string2: String): Int ={
     if(string1 == string2) return 0
     if(string1.length() ==0)
       return string2.length()
     if(string2.length() == 0)
       return string1.length
       
     var dist: Array[Array[Int]] = Array.empty
   }
   * /
   */
}