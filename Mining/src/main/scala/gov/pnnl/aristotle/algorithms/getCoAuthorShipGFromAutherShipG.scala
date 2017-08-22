/**
 *
 * @author puro755
 * @dAug 13, 2017
 * @Mining
 */
package gov.pnnl.aristotle.algorithms

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author puro755
 *
 */
object getCoAuthorShipGFromAutherShipG {

  val sc = SparkContextInitializer.sc
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    //read (paper01 4 author1) RDD
    val paperAuthorRDD =
      sc.textFile("authorGraphDir2010_Final2/part*").filter(f =>
        f.split("\t")(1).equalsIgnoreCase("4") == true).map { line =>
        val arr = line.split("\t")
        (arr(0), arr(1))
      }

    val paperAuthorSet = paperAuthorRDD.map(p =>
      (p._1, Set(p._2))).reduceByKey((set1, set2) =>
      set1 ++ set2)

    val coAuthorShip = paperAuthorSet.flatMap(p => {
      var coAuth: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty

      val tmpList = p._2.toList
      val size = tmpList.size
      if (size > 1) {
        for (i <- 0 until size - 1) {
          for (j <- i + 1 until size)
            coAuth += (tmpList(i) +"_"+ tmpList(j))
        }

      }
      coAuth
    }).distinct

    coAuthorShip.map(entry => entry.split("_")(0) + "\t" + "12" + "\t" + entry.split("_")(1)).saveAsTextFile("coAuthorGraphDir2010_Final2")
  }

}