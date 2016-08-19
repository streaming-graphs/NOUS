/**
 *
 * @author puro755
 * @dJul 6, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.v3

import java.io.Serializable
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File


/**
 * @author puro755
 *
 */
class WindowMetrics extends Serializable {

  /*
   * pattern key with its support in every batch
   * Example : (List(1, 1, 46),List((13,1), (14,1), (15,1), (16,1), (17,1), (18,0), (19,0)))
   * key : List(1, 1, 46) is a pattern
   * value : tuple of (batch_id, support value)
   */
  var pattern_in_this_winodw : RDD[(List[Int], List[(Int,Long)])] = null
  
  
  /*
   * pattern key with its participating nodes in each batch.
   * Example: (List(1, 1, 3),Set((3,0), (2,8)))
   * key : List(1, 1, 3) is a pattern
   * value : Set of tuple (node_degree, node_id) such as Set((3,0), (2,8)))
   */
  var pattern__node_association_window : RDD[(List[Int], Set[(Int,Int)])] = null
  
  
  
  /*
   * Every node with its associated patterns in whole window
   * Example : (21,Set(List(1, 1, 18), List(1, 1, 3), List(1, 1, 18, 1, 1, 3)))
   * key : 21 is a node
   * value : Set(List(1, 1, 18), List(1, 1, 3), List(1, 1, 18, 1, 1, 3))) is a
   * 		set of patterns in the window
   */
  var node_pattern_association_window : RDD[(Int, Set[List[Int]])] = null
  
  
  
  /*
   * Every node with its associated patterns in every batch
   * Example :  (33,Set((9,Set(List(1, 1, 10), List(1, 1, 10, 1, 1, 24), List(1, 1, 10, 1, 1, 3), List(1, 1, 24, 1, 1, 3), List(1, 1, 3), List(1, 1, 24), List(1, 1, 10, 1, 1, 24, 1, 1, 3)))))
   * key: 33 is a node
   * value : Set((9,Set(List(1, 1, 10), List(1, 1, 10, 1, 1, 24), .....) is set of pattern in batch_id=9 . This example has
   * patterns only in one batch.
   */
  var node_pattern_association_per_batch : RDD[(Int, Set[(Int, Set[List[Int]])]) ] = null
  
  val batch_id_map : Map[Int,(Long,Long)] = Map.empty
  val outputfile  = new PrintWriter( new File( "GraphMiningWindowMetrics.txt" ) )

  def updateWindowMetrics(batch_metrics : BatchMetrics)
  {
    val batch_id = batch_metrics.id

    if(pattern_in_this_winodw!=null)
    {
      val joined_pattern_metrics = pattern_in_this_winodw.fullOuterJoin( batch_metrics.pattern_in_this_batch )
      this.pattern_in_this_winodw = joined_pattern_metrics.map( pattern => ( pattern._1, pattern._2._1.getOrElse( List.empty ) ++ List( ( batch_id, pattern._2._2.getOrElse(0L) ) ) ) )
    }
    else
    {
      this.pattern_in_this_winodw = batch_metrics.pattern_in_this_batch.map(pattern => (pattern._1,List((batch_id,pattern._2))))
    }

    if(pattern__node_association_window!=null)
    {
      val join_node_metrics = pattern__node_association_window.fullOuterJoin(batch_metrics.pattern__node_association)
      this.pattern__node_association_window = join_node_metrics.map(node => (node._1, node._2._1.getOrElse(Set.empty) ++ node._2._2.getOrElse(Set.empty)))
    } else
    {
      this.pattern__node_association_window = batch_metrics.pattern__node_association
    }
    
    
    if(node_pattern_association_window!=null)
    {
    	    val join_node_pattern_metrics = node_pattern_association_window.fullOuterJoin(batch_metrics.node_pattern_association)
    	    		this.node_pattern_association_window = join_node_pattern_metrics.map(node 
        => (node._1, node._2._1.getOrElse(Set.empty) ++ node._2._2.getOrElse(Set.empty)))
    }else
    {
      this.node_pattern_association_window = batch_metrics.node_pattern_association
    }
    
    
    if(node_pattern_association_per_batch!=null)
    {
      val join_node_pattern_metrics = node_pattern_association_per_batch.fullOuterJoin( batch_metrics.node_pattern_association.map( node_pattern => ( node_pattern._1, Set( ( batch_id, node_pattern._2 ) ) ) ) )
      this.node_pattern_association_per_batch = join_node_pattern_metrics.map( node => ( node._1, node._2._1.getOrElse( Set.empty ) ++ node._2._2.getOrElse( Set.empty ) ) )
    }
    else
    {
      this.node_pattern_association_per_batch = batch_metrics.node_pattern_association.map(node_pattern 
          => (node_pattern._1,Set((batch_id,node_pattern._2))))
    }
    
  }
  
  
  def saveWindowMetrics()
  {
    /*
    pattern_in_this_winodw.saveAsTextFile("WindowPatternSummary" + System.nanoTime())
    pattern__node_association_window.saveAsTextFile("WidnowPatternNodeAssociation" + System.nanoTime())
    node_pattern_association_window.saveAsTextFile("WindowNodePatternAssociation" + System.nanoTime())
    node_pattern_association_per_batch.saveAsTextFile("BatchNodePatternAssociation" + System.nanoTime())
    * 
    */
    outputfile.println(pattern_in_this_winodw.first.toString)
    outputfile.println(pattern__node_association_window.first.toString)
    outputfile.println(node_pattern_association_window.first.toString)
    outputfile.println(node_pattern_association_per_batch.first.toString)
    outputfile.println("SUCCESS")
    outputfile.flush()
  }
}