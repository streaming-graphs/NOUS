package gov.pnnl.aristotle.text
import org.scalatest._
import gov.pnnl.aristotle.text.LanguageDetector

class LanguageDetectorTest extends FlatSpec {

  ignore  should "return false"  in 
  // "LanguageDetector.isEnglish(resources/multi_lingual/spanish.txt" should "return false"  in 
  {
    val doc = "resources/multi_lingual/spanish.txt"
    val isEnglish = LanguageDetector.isEnglish(doc)
    assert(isEnglish == false)
  }

  ignore should "return true"  in 
  // "LanguageDetector.isEnglish(resources/multi_lingual/english.txt" should "return true"  in 
  {
    val doc = "resources/multi_lingual/english.txt"
    val isEnglish = LanguageDetector.isEnglish(doc)
    assert(isEnglish)
  }
}
