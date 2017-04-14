package gov.pnnl.aristotle.text
import org.scalatest._
import edu.stanford.nlp.pipeline.Annotation
import gov.pnnl.aristotle.text._

class TripleParserTest extends FlatSpec {
/*
  "NumSentences(Child paints picture)" should "return 1"
  {
    val doc1 = "Child paints picture"
    val n1 = TripleParser.getSentenceCount(doc1)
    assert(n1 == 1)
    val doc2 = "Prime Air is a future delivery system from Amazon. It has great potential"
    val n2 = TripleParser.getSentenceCount(doc1)
    assert(n2 == 2)
  }
*/

  // "CorefTransform(<Prime Air is a future delivery system from Amazon. It has great potential.>)" should "return <Prime Air is a future delivery system from Amazon. Prime Air has great potential.>" in
  ignore should "return <Prime Air is a future delivery system from Amazon. Prime Air has great potential.>" in
  {
    val doc = "Prime Air is a future delivery system from Amazon. It has great potential."
    val annotation = TripleParser.getAnnotation(doc)
    val outText = TripleParser.corefTransform(annotation)
    assert(outText == "Prime Air is a future delivery system from Amazon. Prime Air has great potential.")
  }

  "TripleExtractor(Obama was born in Hawaii)" should "return <Obama,was born in,Hawaii>" in 
  {
    val doc1 = "Obama was born in Hawaii"
    val triples1 = TripleParser.getTriples(doc1)
    triples1.foreach(println)
    val triple1_0 = triples1(0)
    assert(triple1_0.sub == "Obama")
    assert(triple1_0.pred == "was born in")
    assert(triple1_0.obj == "Hawaii")
  }

  
}
