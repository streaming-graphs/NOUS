/**
 *
 * @author puro755
 * @dJul 5, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.v3

/**
 * @author puro755
 *
 */
object FilterHeuristics {

   def checkcross_join(pattern1: String, pattern2: String): Boolean =
    {
      return !(pattern1.split("\t")(0).equalsIgnoreCase(pattern2.split("\t")(0)))
    }

  def non_overlapping(pattern1: String, pattern2: String): Boolean =
    {
      val pattern1array = pattern1.replaceAll("\t+", "\t").split("\t")
      val pattern2array = pattern2.replaceAll("\t+", "\t").split("\t")
      val p1a_length = pattern1array.length
      val p2a_length = pattern2array.length

      if (pattern1.contains("person\tworks_at\tcompany")) {
        //println("found")
      }

      if (p1a_length % 3 != 0 || p2a_length % 3 != 0) {
        println(pattern1 + " wrong formatting and " + pattern2)
        //System.exit(1)
      }
      //check 4 combinations of 'boundary-edge' overlap
      // a1b1, a1bn, anb1, anbn
      if ((pattern1array(0).equalsIgnoreCase(pattern2array(0)) &&
        pattern1array(1).equalsIgnoreCase(pattern2array(1)) &&
        pattern1array(2).equalsIgnoreCase(pattern2array(2))) ||

        (pattern1array(0).equalsIgnoreCase(pattern2array(p2a_length - 3)) &&
          pattern1array(1).equalsIgnoreCase(pattern2array(p2a_length - 2)) &&
          pattern1array(2).equalsIgnoreCase(pattern2array(p2a_length - 1))) ||

          (pattern1array(p1a_length - 3).equalsIgnoreCase(pattern2array(0)) &&
            pattern1array(p1a_length - 2).equalsIgnoreCase(pattern2array(1)) &&
            pattern1array(p1a_length - 1).equalsIgnoreCase(pattern2array(2))) ||

            (pattern1array(p1a_length - 3).equalsIgnoreCase(pattern2array(p2a_length - 3)) &&
              pattern1array(p1a_length - 2).equalsIgnoreCase(pattern2array(p2a_length - 2)) &&
              pattern1array(p1a_length - 1).equalsIgnoreCase(pattern2array(p2a_length - 1)))) return false
      return true
    }

  /*
   * Helper function to make sure type patterns are not getting joined to instance
   * pattern.
   * It check some basic situations
   */
  def compatible_join(pattern1: String, pattern2: String): Boolean =
    {
      //if(pattern1.contains("type:person\tfriends_with"))
      //println("found")
      val basetpe: Set[String] = Set("type:person", "type:company", "type:product", "type:sup_mt")
      val pattern1_array = pattern1.split("\t")
      val pattern2_array = pattern2.split("\t")
      //check non-compatible first edge
      if ((pattern1_array(1).equalsIgnoreCase(pattern2_array(1)))
        && (pattern1_array(2).startsWith("type:") && !pattern2_array(2).startsWith("type:"))) return false
      //type:person works_at        type:company      type:company        makes   type:sup_mt5
      //type:person works_at        o5      o5  makes       type:sup_mt5    :4
      else if ((pattern1_array(1).equalsIgnoreCase(pattern2_array(1)))
        && (!pattern1_array(2).startsWith("type:") && pattern2_array(2).startsWith("type:"))) return false
      //check non-compatible last edge
      //type:person     buys    type:product            type:person     works_at        type:company
      //type:person     works_at        o5              o5      makes   type:su    p_mt5    :4
      else if ((pattern1_array(pattern1_array.length - 2).equalsIgnoreCase(pattern2_array(1)))
        && ((pattern1_array(pattern1_array.length - 1).startsWith("type:") && !pattern2_array(2).startsWith("type:"))
          || (!pattern1_array(pattern1_array.length - 1).startsWith("type:") && pattern2_array(2).startsWith("type:")))) return false
      //pattern 2 originates from this node and its first edge is an instance edge of pattern 1's last edge  
      else if ((pattern1_array(1).equalsIgnoreCase(pattern2_array(pattern2_array.length - 2)))
        && ((pattern1_array(2).startsWith("type:") && !pattern2_array(pattern2_array.length - 1).startsWith("type:")) ||
          (!pattern1_array(2).startsWith("type:") && pattern2_array(pattern2_array.length - 1).startsWith("type:")))) return false

      else if (pattern1_array.last.startsWith("type:") && pattern2_array.last.startsWith("type:")) return true
      else if (pattern1_array.last.startsWith("type:") && !basetpe.contains(pattern2_array.last)) return true
      else if (!pattern1_array.last.startsWith("type:") && !basetpe.contains(pattern2_array.last)) return true
      return false
    }
}