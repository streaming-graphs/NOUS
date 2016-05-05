package gov.pnnl.aristotle.text
import gov.pnnl.aristotle.text.datasources.WebFeatureExtractor
import gov.pnnl.aristotle.text.TripleParser

object RelationMiner {
  def main(args: Array[String]) = {
    if (args.length == 0) {
      println("Missing argument: input-file-list subj obj")
      System.exit(1)
    }
    val files = scala.io.Source.fromFile(args(0)).getLines
    val arg1 = args(1)
    val arg2 = args(2)
    val matchPred = args(3).toInt

    var trainingDocs = scala.collection.mutable.Set[String]()
    for (f <- files) {
      val lines = WebFeatureExtractor.getText(f).split("\n")
      for (line <- lines) {
        if (line.contains(arg1) && line.contains(arg2)) {
          val triples = TripleParser.getTriples(line)
          for (t <- triples) {
            if (matchPred > 0) {
              // if (t.sub.contains(arg1) && t.pred.contains(arg2) && trainingDocs.contains(t.pred) == false) {
              if (t.pred.contains(arg2) && trainingDocs.contains(t.pred) == false) {
                println("RULE => " + t.sub + "," + t.pred + "," + t.obj)
                println("+++++++++++++++++++++++++++++++++++++++")
                println("Match found for : " + t.sub + " -> " + t.obj)
                println("---------------------------------------")
                println("DOC: " + f)
                println("---------------------------------------")
                println(line)
                println("---------------------------------------")
                trainingDocs += t.obj
              }
            }
            else {
              if (t.sub.contains(arg1) && t.obj.contains(arg2) && trainingDocs.contains(t.pred) == false) {
                println("RULE => " + t.sub + "," + t.pred + "," + t.obj)
                println("+++++++++++++++++++++++++++++++++++++++")
                println("Match found for : " + t.sub + " -> " + t.obj)
                println("---------------------------------------")
                println("DOC: " + f)
                println("---------------------------------------")
                println(line)
                println("---------------------------------------")
                trainingDocs += t.pred
              }
            }
          }
          if (trainingDocs.size == 10) {
            System.exit(0)
          }
        }
      }
    }
  }
}
