package gov.pnnl.aristotle.text
import org.scalatest._
import edu.stanford.nlp.pipeline.Annotation
import gov.pnnl.aristotle.text._

class TripleParserTest extends FlatSpec {
  "CorefTransform(<Prime Air is a future delivery system from Amazon. It has great potential.>)" should "return <Prime Air is a future delivery system from Amazon. Prime Air has great potential.>" in
  {
    val doc = "Prime Air is a future delivery system from Amazon. It has great potential."
    val annotation = TripleParser.getAnnotation(doc)
    val outText = TripleParser.corefTransform(annotation)
    assert(outText == "Prime Air is a future delivery system from Amazon. Prime Air has great potential.")
  }

  "SRL(Child paints picture)" should "return <Child,paint,picture>" in 
  {
    val doc = "Child paints picture"
    val annotation = TripleParser.getAnnotation(doc)
    val srlTriples = new TripleParser.SemanticRoleLabelExtractor().extract(annotation)
    val triple0 = srlTriples(0)
    assert(triple0.sub == "Child")
    assert(triple0.pred == "paint")
    assert(triple0.obj == "picture")
  }

  
}
