/**
 *
 * @author puro755
 * @dJun 17, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.datamodel

import scala.collection.mutable.LinkedList
import java.util.ArrayList
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.SortedSet
import java.util.TreeSet


/**
 * @author puro755
 *
 */
class PatternGraph {

  var adj : Array[TreeSet[String]] = null
  var v_map : Map[String,Int] = Map.empty
  
  def ConstructPatternGraph(pattern : String)
  {
    val pattern_array = pattern.replaceAll("\t+", "\t").split("\t")
    
    for(i <- 0 to pattern_array.length-1)
      if((i%3==0) || (i%3==2))
          v_map = v_map + (pattern_array(i)->0)
    val v_map_keys = v_map.keys.toArray    
    for(i <- 0 to v_map_keys.length-1)
    {
      v_map = v_map + (v_map_keys(i) -> i)
    }
 
    //Build the adj list graph now
    this.adj = new Array[TreeSet[String]](v_map.size)
    //val emptyset : TreeSet[String] = new TreeSet()
    // When a constant value emptyset is added to each array elemetn,
    // its reference is shared and when later one value is changed, all get updated.
    // so create new local val in the map function and use it.
    this.adj = this.adj.map(f=>{val emptyset : TreeSet[String] = new TreeSet()
    emptyset})
    for(i <- 0 until (pattern_array.length-1) by 3 )
    {
      if(i%3==0)//its a source node
      {
        val subjectid = v_map.getOrElse(pattern_array(i),-1)
        this.adj(subjectid).add((pattern_array(i) + "\t"+pattern_array(i+1)+"\t"+ pattern_array(i+2)))
      }
    }
    
  }
  
      // A function used by DFS
    def DFSUtil(v : Int,  visited : Array[Boolean],path:ListBuffer[String])
    {
        // Mark the current node as visited and print it
        visited(v) = true;
        //System.out.print(v+" ");
 
        // Recur for all the vertices adjacent to this vertex
        val adjset = adj(v).iterator
        while(adjset.hasNext())
        {
          val netxadj = adjset.next()
          val nbr_id : Int = v_map(netxadj.split("\t")(2))
          if (!visited(nbr_id))
            {
            path += netxadj
            DFSUtil(nbr_id, visited,path);
            }

        }
      
    }
    
   def DFS(v : String) : String = 
    {
        // Mark all the vertices as not visited(set as
        // false by default in java)
        val visited : Array[Boolean] = new Array[Boolean](v_map.size);
   			 var path : ListBuffer[String] = ListBuffer.empty
        // Call the recursive helper function to print DFS traversal
   			DFSUtil(v_map.getOrElse(v, -1), visited,path);
   			return path.toList mkString("\t")
    }
}