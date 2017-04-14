package gov.pnnl.aristotle.text.datasources
import org.apache.spark._

object UrlSourceFinder {

  def getKeys(urlStr:String): Array[String] = {
    val url = new java.net.URL(urlStr)
    val query = url.getQuery()
    if (query != null) query.split("&").map(_.split("=")(0))
    else Array[String]()
  }

  def run(inPath: String, outPath: String) = {
    val sc = new SparkContext(new SparkConf().setAppName("UrlSourceFinder"))
    // val lines = scala.io.Source.fromFile(inPath).getLines.toList
    val lines = sc.textFile(inPath)
    val urls = lines.map(_.split("\t")(1))
    println("\n\n\nLoaded " + urls.count + " URLs")
    val hosts = urls.map(new java.net.URL(_).getHost)
                    .groupBy(identity).mapValues(_.size)
    hosts.saveAsTextFile(outPath)
    /*
    val outWriter = new java.io.PrintWriter(outPath) 
    hosts.foreach(outWriter.println)
    outWriter.close()
    */
  }

  def main(args: Array[String]) = {
    run(args(0), args(1))
  }
}
