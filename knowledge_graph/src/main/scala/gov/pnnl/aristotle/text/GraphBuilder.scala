package gov.pnnl.aristotle.text
import gov.pnnl.aristotle.text.datasources._
import org.apache.spark._
import java.io.File
import java.io.PrintWriter


object GraphBuilder {
  def main(args: Array[String]) = {
    val fileList = args(0)
    val outPath = args(1)
    val lines = scala.io.Source.fromFile(args(0)).getLines.toList

    val sc = new SparkContext(new SparkConf()
                                  .setMaster("local[2]")
                                  .setAppName("TripleParser"))
    val inputListRDD = sc.textFile(args(0))

    val urlTextPairs = inputListRDD.flatMap(path => SimpleDocParser.getUrlTextPairs(path))
    val triples = urlTextPairs.flatMap(urlTextPair => 
        TripleParser.getTriples(urlTextPair._2))

    // val pw = new java.io.PrintWriter(new File(outPath))
    // triples.foreach(triples => pw.println(triples.mkString("_")))
    // triples.foreach(t => pw.println(t))
    // pw.close()
    // tripleParser.srlOutputWriter.close()

    triples.saveAsTextFile(outPath)
  }
}
