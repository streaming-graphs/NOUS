package gov.pnnl.aristotle.text
import org.apache.tika.config.TikaConfig
import org.apache.tika.metadata.Metadata
import org.apache.tika.io.TikaInputStream

import org.apache.tika.language.LanguageIdentifier

object LanguageDetector  {
  def isEnglish(text: String): Boolean = {
    val lang = new LanguageIdentifier(text).getLanguage
    // println("LANGUAGE = " + lang)
    if (lang == "en")
      true
    else
      false
  }

  def getLanguageDistribution(inputFileList: String): Map[String, Int] = {
    val files = scala.io.Source.fromFile(inputFileList).getLines.toList
    val languageIds = files.map(f => new LanguageIdentifier(
                                          scala.io.Source.fromFile(f).getLines.mkString
                                        ).getLanguage)
    languageIds.groupBy(w => w).mapValues(_.size)
  }

  def main(args: Array[String]) = {
    if (args.length == 0) {
      println("ERROR Missing argument: Specify a file with each line containing a file path")
      System.exit(1)
    }
    val languageCounts = getLanguageDistribution(args(0))
    print(languageCounts)
  }
}
