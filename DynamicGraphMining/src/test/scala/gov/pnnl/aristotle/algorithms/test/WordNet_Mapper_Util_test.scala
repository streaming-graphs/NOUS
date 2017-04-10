/**
 *
 */
package gov.pnnl.aristotle.algorithms.test

import org.scalatest.FlatSpec
import gov.pnnl.aristotle.algorithms.WordNet_Mapper_Util
import org.scalatest._
/**
 * @author puro755
 *
 */
class WordNet_Mapper_Util_test extends FlatSpec {

  "writing" should "return wrote" in {
    assert("wrote".equals(WordNet_Mapper_Util.getCanonicalVerb("writing")))
  }
}