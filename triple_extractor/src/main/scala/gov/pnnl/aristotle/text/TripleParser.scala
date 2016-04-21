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
import edu.stanford.nlp.naturalli.NaturalLogicAnnotations.RelationTriplesAnnotation
import edu.stanford.nlp.ie.util.RelationTriple;
import edu.stanford.nlp.simple._;

case class Triple(val sub: String, val pred: String, val obj: String, val conf: Double) {
  override def toString(): String = {
    val sbuf = new StringBuilder()
    sbuf.append(sub).append("\t")
        .append(pred).append("\t")
        .append(obj).append("\t")
        .append(conf)
    sbuf.toString
  }
}

object TripleParser extends Serializable {

  private val props = new Properties()
  println("$$$$$$$$$$$ LOADING CORENLP MODELS")
  props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,depparse,mention,coref,natlog,openie")
  props.setProperty("openie.resolve_coref", "true")
  props.setProperty("openie.triple.all_nominals", "false")
  props.setProperty("openie.ignore_affinity", "true")

  private val badVerbs = Set("was")
  // props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,mention,coref")
  private val pipeline = new StanfordCoreNLP(props)

  private val propsWithoutCoref = new Properties()
  propsWithoutCoref.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,depparse,mention,natlog,openie")
  // propsWithoutCoref.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,mention")
  private val pipelineWithoutCoref = new StanfordCoreNLP(propsWithoutCoref)

  // val srlOutputWriter = new java.io.PrintWriter(new java.io.File("srl.txt"))

  def getPipeline(): StanfordCoreNLP = pipeline

  // def getAnnotation(doc: String): Annotation = pipeline.process(doc)

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

  object NamedPhraseExtractor {

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
          val nerLabel = t.get(classOf[NamedEntityTagAnnotation])
          // println("[" + nerLabel + "] TYPE OF [" + word + "]")
          val isNP = isNounPhrase(pos)
          if (isNP) {
            if (npList.size == 0) {
              npList += (nerLabel + ":" + word)
            }
            else {
              npList += word
            }
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

  object OpenIEExtractor {
    def extractFiltered(annotation: Annotation, namedPhrasesWithTags: Set[String]): List[Triple] = {

      def getNamedLabelMap(namedPhrases: Set[String]): Map[String, String] = {
        namedPhrases.map(nerLabelNamePair => {
          val tokens = nerLabelNamePair.split(":")
          (tokens(1), tokens(0))
        }).toMap 
      }

      def getNerTaggedName(nameLabelMap: Map[String, String], 
                    namedPhrases: Set[String],
                    name: String): String = {
        for (np <- namedPhrases) {
          if (name.contains(np)) return (nameLabelMap(np) + ":" + np)
        }
        ""
      }

      val nameLabelMap = getNamedLabelMap(namedPhrasesWithTags)
      val namedPhrases = nameLabelMap.keySet
      val sentences = annotation.get(classOf[SentencesAnnotation])
      val tripleBuffer = new ListBuffer[Triple]()
      for (s <- sentences) {
        // println("***" + s)
        val sTriples = s.get(classOf[RelationTriplesAnnotation])
        for (t <- sTriples) {
          // println("---> " + t)
          val sub = t.subjectGloss()
          val obj = t.objectGloss()
          val relation = t.relationGloss()
          tripleBuffer += Triple(sub, relation, obj, t.confidence) 
        }
      }
      tripleBuffer.toList.filter(t => TripleFilter.filter(t, namedPhrases))
    }

    def extract(annotation: Annotation, namedPhrasesWithTags: Set[String]): List[Triple] = {

      val sentences = annotation.get(classOf[SentencesAnnotation])
      val tripleBuffer = new ListBuffer[Triple]()
      for (s <- sentences) {
        val sTriples = s.get(classOf[RelationTriplesAnnotation])
        for (t <- sTriples) {
          val sub = t.subjectGloss()
          val obj = t.objectGloss()
          val relation = t.relationGloss()
          tripleBuffer += Triple(sub, relation, obj, t.confidence) 
        }
      }
      tripleBuffer.toList
    }
  }

  // def getCorefedAnnotation(doc: String): Annotation = {
  def getAnnotation(doc: String): Annotation = {
    try {
      pipeline.process(doc)
      // val transformedText = new CorefTransform().transform(annotation)
      // pipeline.process(transformedText)
    } catch {
      case ex: java.lang.RuntimeException => {
        println("CAUGHT EXCEPTION FOR: " + doc)
        pipelineWithoutCoref.process(doc)
      }
    }
 
  }

  /*def getAnnotation1(doc: String): Annotation = {
    if (doc.indexOf(".") == -1) 
      pipelineWithoutCoref.process(doc)
    else 
      getCorefedAnnotation(doc)
  }*/

  def getTriples(doc: String): List[Triple] = {
    
    println(" In getTriples, doc string= ", doc)
    val annotation = getAnnotation(doc)
    val namedPhrases = NamedPhraseExtractor.extract(annotation)
    println("calculated name phrases", namedPhrases.toString)
    //val srlTriples = new SemanticRoleLabelExtractor().extract(annotation)
    val openieTriples = OpenIEExtractor.extractFiltered(annotation, namedPhrases)
    println(" calculated triples from openIE", openieTriples.toString)
    openieTriples 
  }

  def getTriples1(doc: String): List[Triple] = {
    val document = new Document(doc)
    val tripleBuffer = new ListBuffer[Triple]()
    for (sent <- document.sentences()) {
      for (triple <- sent.openieTriples()) {
        tripleBuffer += Triple(triple.subjectLemmaGloss(), triple.relationLemmaGloss(), triple.objectLemmaGloss(), triple.confidence) 
      }
    }
    tripleBuffer.toList
  }
}
