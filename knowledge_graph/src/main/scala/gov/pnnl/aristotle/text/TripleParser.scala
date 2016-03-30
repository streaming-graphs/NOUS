package gov.pnnl.aristotle.text 

import edu.stanford.nlp.hcoref.CorefCoreAnnotations
import edu.stanford.nlp.hcoref.data.Dictionaries.MentionType
import edu.stanford.nlp.hcoref.data.CorefChain
import edu.stanford.nlp.hcoref.data.Mention
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.ling.CoreAnnotations._
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import edu.uw.easysrl.main.EasySRLProcessor

case class Triple(val sub: String, val pred: String, val obj: String) {
  override def toString(): String = {
    val sbuf = new StringBuilder()
    sbuf.append(sub).append("->")
        .append(pred).append("->")
        .append(obj)
    sbuf.toString
  }
}

object TripleParser extends Serializable {

  private val props = new Properties()
  props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,mention,coref")
  private val pipeline = new StanfordCoreNLP(props)
  private val srlProcessor = new EasySRLProcessor()
  // val srlOutputWriter = new java.io.PrintWriter(new java.io.File("srl.txt"))

  def getPipeline(): StanfordCoreNLP = pipeline

  def getAnnotation(doc: String): Annotation = pipeline.process(doc)

  class CorefTransform {
    case class Span(target: String, replace: String)

    val editBuf = scala.collection.mutable.Map[Int, ListBuffer[Span]]()
    val editList = scala.collection.mutable.Map[Int, List[Span]]()

    private def add(sentNum: Int, target: String, replace: String) = {
      if (editBuf.contains(sentNum)) {
        editBuf(sentNum) += Span(target, replace)
      }
      else {
        editBuf += (sentNum -> ListBuffer[Span](Span(target, replace)))
      } 
    }

    private def buildEditList() = {
      for (sentNum <- editBuf.keys) editList += (sentNum -> editBuf(sentNum).toList)
    }

    private def replaceCorefs(sentId: Int, origSentence: String): String = {
      var outText = origSentence
      if (editList.contains(sentId)) {
        val edits = editList(sentId)
        if (edits != None) {
          for (e <- edits) outText = outText.replace(e.target, e.replace) 
        }
      }
      outText
    }

    def transform(annotation: Annotation): String = {
      val corefChains = annotation.get(classOf[CorefCoreAnnotations.CorefChainAnnotation])
      for (e <- corefChains.entrySet) {
        val mention = e.getValue.getRepresentativeMention().mentionSpan
        for (r <- e.getValue.getMentionsInTextualOrder()) {
          if (r.mentionType == MentionType.valueOf("PRONOMINAL")) {
            this.add(r.sentNum, r.mentionSpan, mention)
          }
        } 
      }
      this.buildEditList()
      var sentId = 1
      var sentenceMap = scala.collection.immutable.TreeMap[Int, String]()
      for (s <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
        val outText = replaceCorefs(sentId, s.toString)
        sentenceMap += (sentId -> outText)
        sentId += 1
      }
      sentenceMap.values.toList.mkString(" ")  
    }
  }

  def corefTransform(annotation: Annotation): String = new CorefTransform().transform(annotation)

  class NamedPhraseExtractor {

    def extract(annotation: Annotation): Set[String] = { 

      def isNounPhrase(pos: String): Boolean = { 
        // if (pos == "NN" || pos == "NNS" || pos == "NNP" || pos == "NNPS") true else false
        if (pos == "NNP" || pos == "NNPS") true else false
      }   
  
      val sentences = annotation.get(classOf[SentencesAnnotation])
      var isLastWordNP = false
      var npList = new ListBuffer[String]()
      val namedPhrases = new ListBuffer[String]()
  
      for (s <- sentences) {
        for (t <- s.get(classOf[TokensAnnotation])) {
          val word = t.get(classOf[TextAnnotation])
          val pos = t.get(classOf[PartOfSpeechAnnotation])
          // println(word + " -> " + pos)
          val isNP = isNounPhrase(pos)
          if (isNP) {
            npList += word
          }   
          else if (isNP == false && isLastWordNP) {
            namedPhrases += npList.toList.mkString(" ")
            npList = new ListBuffer[String]()
          }   
          isLastWordNP = isNP
        }   
        if (isLastWordNP) {
          namedPhrases += npList.toList.mkString(" ")
        }   
        isLastWordNP = false
        npList = new ListBuffer[String]()
      }   
  
      namedPhrases.toList.toSet 
    }
  
    def extract(doc: String): Set[String] = { 
      val annotation = pipeline.process(doc)
      extract(annotation)
    }
  }


  class SemanticRoleLabelExtractor {
    val SRLExpr = """(\w+)\s(ARG\d)\s(\w+)""".r

    def extract(annotation: Annotation, debug: Boolean = false): List[Triple] = {
      val sentences = annotation.get(classOf[SentencesAnnotation])
      var triples = List.empty[Triple]
      for (s <- sentences) {
        if (debug) println("INPUT: " + s.toString)
        // srlOutputWriter.println("INPUT")
        // srlOutputWriter.println(s.toString)
        val output = srlProcessor.process(s.toString)
        // srlOutputWriter.println("OUTPUT")
        // srlOutputWriter.println(output)
        val lines = output.split("\n")
        //val lines = srlProcessor.process(s.toString).split("\n")
        var tripleMap = Map.empty[String, Triple]
        for (srlOutput <- lines) {
          try {
            // val SRLExpr(pred, rel, entity) = srlOutput
            if (debug) println("SRL output: " + srlOutput)
            val tokens = srlOutput.split(" ")
            val rel = tokens(1)
            // if (rel.contains("ARG") != false) {
            if (rel == "ARG0" || rel == "ARG1") {
              val pred = tokens(0)
              val entity = tokens.drop(2).mkString(" ")
              if (tripleMap.contains(pred) == false) {
                if (rel == "ARG0") 
                  tripleMap = tripleMap + (pred -> Triple(entity, pred, ""))
                else 
                  tripleMap = tripleMap + (pred -> Triple("", pred, entity))
              }
              else {
                val partialFact = tripleMap(pred)
                if (rel == "ARG0") 
                  tripleMap = tripleMap + (pred -> Triple(entity, pred, partialFact.obj))
                else 
                  tripleMap = tripleMap + (pred -> Triple(partialFact.sub, pred, entity))
              }
              /* if (tripleMap.contains(pred) == false) {
                tripleMap = tripleMap + (pred -> Triple(entity, pred, ""))
              }
              else {
                val partialFact = tripleMap(pred)
                tripleMap = tripleMap + (pred -> Triple(partialFact.sub, pred, entity))
              } */ 
            }
          } catch {
            case obx: java.lang.ArrayIndexOutOfBoundsException => {
              // println("FAILED TO PARSE = " + srlOutput + "]")
            }
            case ex: scala.MatchError => {
              // println("Skipped " + srlOutput)
            }
          }
        }
        triples = triples ::: tripleMap.values.filter(_.obj != "").toList
        // println("INPUT: " + s.toString)
        // triples.foreach(println)
      }
      triples
    }
  }

  def getTriples(doc: String): List[Triple] = {
    val annotation = pipeline.process(doc)
    val transformedText = new CorefTransform().transform(annotation)
    val newAnnotation = pipeline.process(transformedText)
    val namedPhrases = new NamedPhraseExtractor().extract(newAnnotation)
    // println("Printing named phrases ...")
    // namedPhrases.foreach(println)
    val srlTriples = new SemanticRoleLabelExtractor().extract(newAnnotation)
    val triples = srlTriples.filter(t => {
        namedPhrases.exists(n => t.sub.contains(n)) &&
        namedPhrases.exists(n => t.obj.contains(n))
    })
    triples 
  }
}
