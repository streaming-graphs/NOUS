/**
 *
 */
package gov.pnnl.aristotle.algorithms

/**
 * @author puro755
 *
 */
class PatternInstance(var instance:scala.collection.immutable.Set[(Int, Int)]) extends Serializable with Equals {

    //private var instance : scala.collection.immutable.Set[(Int, Int)] = null;
	def get_instacne : scala.collection.immutable.Set[(Int, Int)] = return instance
  
  	//Setter
	def set_instance_= (new_instance:Set[(Int, Int)]):Unit = instance = new_instance
	
	def add_edge_to_instance(new_edge : (Int, Int)):Unit = instance  += new_edge
	
	def merge_instance(new_instance : scala.collection.immutable.Set[(Int, Int)]):Unit = {
	  new_instance.foreach(edge=>instance  += edge )
	}
	
	override def toString(): String = {
	  var resulting_string = "Set(";
	  instance.foreach(f=> resulting_string = resulting_string + " (" + f._1 + " " + f._2 + ")" )
	  resulting_string + ")"
	}
  
  def canEqual(other: Any) = {
	  other.isInstanceOf[gov.pnnl.aristotle.algorithms.PatternInstance]
	}
  
  override def equals(other: Any) = {
	  other match {
	    case that: gov.pnnl.aristotle.algorithms.PatternInstance => 
	      {
	        var result = true;
	        val this_instances = this.get_instacne
	        //TODO: Check if set has stable ordering
	        val other_instances  : scala.collection.immutable.Set[(Int, Int)] = 
	          other.asInstanceOf[gov.pnnl.aristotle.algorithms.PatternInstance].get_instacne
	       
	        //two custom sets are equal if each element of A1 is in A2 and
	          // each element of A2 is in A1
	          //this_instances.foreach(this_instance => )
	          if(this_instances.size != other_instances.size)
	          {
	            result = result && false
	          }
	          else
	          {
            val ti = this_instances.toList.sortWith((edge1, edge2) => edge1._2 > edge2._2)
            val oi = other_instances.toList.sortWith((edge1, edge2) => edge1._2 > edge2._2)
			for(i<-0 to ti.size-1)
			{
			  if ((ti(i)._1 == oi(i)._1)
                  && (ti(i)._2 == oi(i)._2))
                  result = result && true
               else
                  result = result && false
			}
          }
	        that.canEqual(PatternInstance.this) && result
	    }
	    case _ => false
	  }
	}
  
  override def hashCode() = {
	  val prime = 41
	  var hashcode = prime
	  instance.asInstanceOf[scala.collection.immutable.Set[(Int, Int)]].foreach(an_instance =>
	    {
	      hashcode = an_instance._1.hashCode + an_instance._2.hashCode
	    })
	 hashcode
	}

}