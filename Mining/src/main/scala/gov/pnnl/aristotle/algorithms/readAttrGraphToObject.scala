/**
 *
 * @author puro755
 * @dAug 13, 2017
 * @Mining
 */
package gov.pnnl.aristotle.algorithms

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

/**
 * @author puro755
 *
 */
object readAttrGraphToObject {

  val sc = SparkContextInitializer.sc
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  
  def main(args: Array[String]): Unit = {
     /*
     * for MAG, the predicates are :
     * 
     * object predicates {
			  val hasPublishDate = 1
			  val hasPublishYear = 2
			  val hasConfId = 3
			  val hasAuthor = 4
			  //val paperHasAff = 5
			  val authorHasAff = 6
			  //val hasKeyword = 7
			  val hasFieldOfStudy = 8
			  val cites = 9
			  val hasType = 10
			  
			 //NEW
			  * hasReputation = 11
			  * hasFOS = 12 //clustered fos
			  }
     * 
     */
    
    //1read citation grpah
    val paperCitePaper =
      sc.textFile("citationGraphDir2010_Final2/part*").filter(f =>
        f.split("\t")(1).equalsIgnoreCase("9") == true).flatMap { line =>
        var edgeSet : scala.collection.mutable.Set[(Int,Int,Int)] = scala.collection.mutable.Set.empty
          val arr = line.split("\t")
          edgeSet += ((arr(0).toInt, 10, 0))//paper type is 0
          edgeSet += ((arr(2).toInt, 10, 0))
          edgeSet += ((arr(0).toInt, 9, arr(2).toInt))
          edgeSet
      }

        val paperAttr =
      sc.textFile("paperAttributeDir2010_Final2/part*").filter(f =>
        f.split("\t")(2).equalsIgnoreCase("-1") == false).flatMap { line =>
        var edgeSet : scala.collection.mutable.Set[(Int,Int,Int)] = scala.collection.mutable.Set.empty
          val arr = line.split("\t")
          //edgeSet += ((arr(0).toInt, 10, 1)) It should be there, why create another edge
          edgeSet += ((arr(1).toInt, 10, (arr(1).toInt)))
          edgeSet += ((arr(2).toInt, 10, (arr(2).toInt)))
          edgeSet += ((arr(0).toInt, 11, arr(1).toInt))
          edgeSet += ((arr(0).toInt, 12, arr(2).toInt))
          edgeSet
      }.distinct

       val paperByAuthor =
      sc.textFile("authorGraphDir2010_Final2/part*").filter(f =>
        f.split("\t")(1).equalsIgnoreCase("4") == true).flatMap { line =>
        var edgeSet : scala.collection.mutable.Set[(Int,Int,Int)] = scala.collection.mutable.Set.empty
          val arr = line.split("\t")
          //edgeSet += ((arr(0).toInt, 10, 0)) It should be there
          edgeSet += ((arr(2).toInt, 10, 1)) //author type is 1
          edgeSet += ((arr(0).toInt, 4, arr(2).toInt))
          edgeSet
      }
    
        val authorAttr =
      sc.textFile("paperAttributeDir2010_Final2/part*").filter(f =>
        f.split("\t")(2).equalsIgnoreCase("-1") == false).flatMap { line =>
        var edgeSet : scala.collection.mutable.Set[(Int,Int,Int)] = scala.collection.mutable.Set.empty
          val arr = line.split("\t")
          //edgeSet += ((arr(0).toInt, 10, 1)) It should be there, why create another edge
          edgeSet += ((arr(1).toInt, 10, (arr(1).toInt)))
          edgeSet += ((arr(2).toInt, 10, (arr(2).toInt)))
          edgeSet += ((arr(0).toInt, 11, arr(1).toInt))
          edgeSet += ((arr(0).toInt, 12, arr(2).toInt))
          edgeSet
      }.distinct

      
      val allRDDs = paperCitePaper.union(paperAttr).union(paperByAuthor).union(authorAttr)
      
      val allEdges = allRDDs.map(e=>Edge(e._1, e._2, e._3)).distinct.saveAsObjectFile("full2010Edges")
      val allVertices = allRDDs.flatMap(e=>{
        Set((e._1.toLong, e._1.toLong),(e._3.toLong, e._3.toLong))
      }).distinct.saveAsObjectFile("full2010Vertices")
      //val fullGraph = Graph(allVertices, allEdges)
  }

}