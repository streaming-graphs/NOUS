package gov.pnnl.aristotle.text
import java.io._

object Main {

  def main(args: Array[String]) {
    val doc = "Detroit in January isn't an exotic destination, with wind chills in the teens and snowplows scraping streets in their annual battle with winter. But more than 5,000 journalists, many from overseas, will flock there next week for the preview of the 103rd annual auto show."
    TripleParser.getTriples(doc)
  }
}
