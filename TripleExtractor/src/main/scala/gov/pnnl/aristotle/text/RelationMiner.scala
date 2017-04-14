package gov.pnnl.aristotle.text
import gov.pnnl.aristotle.text.datasources.WebFeatureExtractor
import gov.pnnl.aristotle.text.TripleParser
import gov.pnnl.aristotle.text.Triple
import java.io._

object RelationMiner {
  def checkBootstrapFilter(line: String, t: Triple, bootstrapWithEntities: Boolean): Boolean = {
    if (bootstrapWithEntities) 
      line.contains(t.sub) && line.contains(t.obj)
    else
      line.contains(t.pred)
  }
 
  def checkTriple(t: Triple, filterTriple: Triple, bootstrapWithEntities: Boolean): Boolean = {
    if (bootstrapWithEntities) 
      t.sub.contains(filterTriple.sub) && t.obj.contains(filterTriple.obj)
    else
      t.pred.contains(filterTriple.pred)
  }
 
  def minePredicateRules(
      candidate: Triple, 
      bootstrapWithEntities: Boolean,
      fileList: List[String]): scala.collection.mutable.Map[String, Int] = {

    val ruleSupport = scala.collection.mutable.Map[String, Int]()

    for {
      f <- fileList
      val text = WebFeatureExtractor.get("text", f)
      line <- text.split("\n")
      if checkBootstrapFilter(line, candidate, bootstrapWithEntities)
      t <- TripleParser.getTriples(line)
      if checkTriple(t, candidate, bootstrapWithEntities)
      key = t.sub + "*" + t.pred + "*" + t.obj
    } {
      ruleSupport += (key -> (ruleSupport.getOrElseUpdate(key, 0) + 1))
    }

    ruleSupport
  }

  def bootstrapRules(
      seedPairsFile: String, 
      corpusList: String,
      bootstrapWithEntities: Boolean, 
      outPath: String) = {
    val seedLines = scala.io.Source.fromFile(seedPairsFile).getLines.toList
    val seeds = seedLines.map(line => line.split(","))
    val files = scala.io.Source.fromFile(corpusList).getLines.toList
    for {
      seed <- seeds
      t = if (bootstrapWithEntities) Triple(seed(0), "*", seed(2)) else Triple("*", seed(1), "*")
    } {
      val rulesSupport = minePredicateRules(t, bootstrapWithEntities, files) 
      val writer = new PrintWriter(new BufferedWriter(new FileWriter(outPath, true)))
      for ((rule, support) <- rulesSupport) {
        writer.println(rule + "\t" + support)
      }
      writer.close()
    }
  }

  def extractTriples(corpusList: String, rules: String, outpath: String): List[Triple] = {
    val fileList = scala.io.Source.fromFile(corpusList).getLines.toList
    val tripleBuf = new scala.collection.mutable.ListBuffer[Triple]()
    for {
      f <- fileList
      val text = WebFeatureExtractor.get("text", f)
      line <- text.split("\n")
      // TODO if checkRuleFilter(line, candidate, bootstrapWithEntities)
      t <- TripleParser.getTriples(line)
      // TODO if checkTriple(t, candidate, bootstrapWithEntities)
    } tripleBuf += t
    
    tripleBuf.toList
  }

  def main(args: Array[String]) = {
    if (args.length == 0) {
      println("Missing argument: seedlist-path corpus-filelist-path bootstrap-with-entities out-path")
      System.exit(1)
    }
    val seedFilePath = args(0)
    val corpusFile = args(1)
    val bootstrapWithEntities = args(2).toBoolean
    val outpath = args(3)
    val rules = RelationMiner.bootstrapRules(seedFilePath, corpusFile, bootstrapWithEntities, outpath) 
    
  }
}
