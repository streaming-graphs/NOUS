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
import java.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import edu.stanford.nlp.naturalli.NaturalLogicAnnotations.RelationTriplesAnnotation
import edu.stanford.nlp.ie.util.RelationTriple;
import edu.stanford.nlp.simple._;

import org.json4s._
//import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
//import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._
import org.json4s.native.Serialization
import collection.immutable.ListMap
import scala.util.parsing.json._


object TripleParserSample extends Serializable {

  private val props = new Properties()
  // println("$$$$$$$$$$$ LOADING CORENLP MODELS")
  props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,depparse,mention,coref,natlog,openie")
  // props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner")
  props.setProperty("threads", "8")
  props.setProperty("openie.resolve_coref", "true")
  props.setProperty("openie.triple.all_nominals", "false")
  // props.setProperty("openie.ignore_affinity", "true")

  private val badVerbs = Set("was")
  // props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,mention,coref")
  private val pipeline = new StanfordCoreNLP(props)

  private val propsWithoutCoref = new Properties()
  propsWithoutCoref.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,depparse,mention,natlog,openie")
  // propsWithoutCoref.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner")
  propsWithoutCoref.setProperty("threads", "8")
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
          val isNP = isNounPhrase(pos)
          // println("[" + word + "] POS [" + pos + "]" + "] NER [" + nerLabel + "] isNP [" + isNP + "]")
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
  
  

    def fancyextract(annotation: Annotation): Set[String] = { 

      def isNounPhrase(pos: String): Boolean = { 
         if (pos == "NN" || pos == "NNS" || pos == "NNP" || pos == "NNPS") true else false
        //if (pos == "NN" || pos == "NNS" || pos == "NNP" || pos == "NNPS") true else false
      }   

      def isDT(pos : String): Boolean = {
        if (pos == "DT") true else false
      } 

      def isLRB(pos : String): Boolean = {
        if (pos == "-LRB-") true else false
      }
  
      val sentences = annotation.get(classOf[SentencesAnnotation])
      var isLastWordNP = false
      var npList = new ListBuffer[String]()
      var posList = new ListBuffer[String]()
      val namedPhrases = new ListBuffer[String]()
  
      for (s <- sentences) {
        for (t <- s.get(classOf[TokensAnnotation])) {
          val word = t.get(classOf[TextAnnotation])
          val pos = t.get(classOf[PartOfSpeechAnnotation])
          val nerLabel = t.get(classOf[NamedEntityTagAnnotation])

          // println("[" + word + "] POS [" + pos + "] NER [" + nerLabel + "]")
          if (isDT(pos) || isNounPhrase(pos)) {
            if (npList.size == 0) {
              npList += (nerLabel + ":" + word)
            } else {
              npList += word
            }
            posList += pos
          } else {
            val poses = posList.toList
            val mentions = npList.toList
            if (poses.size > 0) {
              if (poses.head == "-LRB-" || poses.head == "DT") {
                if (mentions.drop(1).size > 1) {
                  namedPhrases += (mentions.head.split(":")(0) + ":" + mentions.drop(1).mkString(" "))
                }
              } else if (poses.head == "NNP" || poses.head == "NNPS") {
                  if (mentions.size > 1) {
                    namedPhrases += mentions.mkString(" ")
                  }
              } 

              posList.clear() 
              npList.clear() 
              if (isLRB(pos)) {
                npList += (nerLabel + ":" + word)
                posList += pos
              }
            }
          } 
        }
      }
      namedPhrases.toList.toSet 
    }
  
    def fancyextract(doc: String): Set[String] = { 
      val annotation = pipeline.process(doc)
      fancyextract(annotation)
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
        if (sTriples != null) {
          for (t <- sTriples) {
            val sub = t.subjectGloss()
            val obj = t.objectGloss()
            val relation = t.relationGloss()
            println("---> " + sub + " -> " + relation + " -> " + obj)
            tripleBuffer += Triple(sub, relation, obj, "", "", t.confidence) 
          }
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
          tripleBuffer += Triple(sub, relation, obj, "", "", t.confidence) 
        }
      }
      tripleBuffer.toList
    }
  }

  // def getCorefedAnnotation(doc: String): Annotation = {
  def getAnnotation(doc: String): Annotation = {
    var t1 = 0L
    try {
      pipeline.process(doc)
      // val transformedText = new CorefTransform().transform(annotation)
      // pipeline.process(transformedText)
    } catch {
      case ex: java.lang.RuntimeException => {
        println("\n\n\n\nCAUGHT java.lang.RuntimeException AT LINE 228")
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

  def reduceGroup(triples: List[Triple]): Triple = {
    // triples.sortWith(_.obj < _.obj).last
    triples.sortWith(_.obj.length < _.obj.length).last
  }

  def purge(triples: List[Triple]): List[Triple] = {
    val groupedTriples = triples.groupBy(t => (t.sub + t.pred)).mapValues(reduceGroup)
    groupedTriples.values.toList
  }
  
  def getTypeTriples(namedEntities: Set[String]): List[Triple] = { 
    val triples = namedEntities.map(e => {
        val tokens = e.split(":")
        Triple(tokens(1), "rdf:type", tokens(0))
      })
    triples.toList
  }

  def getTriples(doc: String): List[Triple] = {
    //println("##########################")
    //println(doc)
    //println("##########################")
    // val t1 = System.currentTimeMillis
    val annotation = getAnnotation(doc)
    // val t2 = System.currentTimeMillis
    val namedPhrases = NamedPhraseExtractor.extract(annotation)
    if (namedPhrases.size() == 0) {
      List[Triple]()
    }
    else {
      println("********** NER output **********")
      namedPhrases.foreach(println)
      // val t3 = System.currentTimeMillis
      //val srlTriples = new SemanticRoleLabelExtractor().extract(annotation)
      val openieTriples = OpenIEExtractor.extractFiltered(annotation, namedPhrases)
      
      println("********** OpenIE output **********")
      openieTriples.foreach(println)
      // val t4 = System.currentTimeMillis
      // println("getAnnotation = " + (t2-t1) + " NER = " + (t3-t2) + " OpenIE = " + (t4-t3))
      val relations = purge(openieTriples.filter(_.conf > 0.98))
      println("********** Purged output **********")
      relations.foreach(println)
      val finalTriples = getTypeTriples(namedPhrases) ::: relations
      println("********** Final output **********")
      finalTriples.foreach(println)
      finalTriples
    }
  }

  object relate {
    def unapply(x : Triple) = Some(x.sub, x.pred, x.obj, x.timestamp, x.src, x.conf) 
  } 

  def tos(t:Triple) : String = {t match {
      //case relate(s,p,o,_,_,_) => s + "\t" + p + "\t" + o
      case relate(s,p,o,_,_,_) => List("<"+s+">", "<"+p+">", "<"+o+">").mkString("  ")
    }
  }

  def getDumpTriples(sentences: List[String], docname: String) = {
    val result = ListMap("doc_name" -> docname)
    var lst = new ListBuffer[ListMap[String,_]]() 
    for (i <- 0 until sentences.size) {
      println(i)
      val doc = sentences(i)
      val annotation = getAnnotation(doc)
      val namedPhrases = NamedPhraseExtractor.extract(annotation)
      if (namedPhrases.size > 0) {
        //val entities = namedPhrases.map(s => s.split(":").last)
        val openieTriples = OpenIEExtractor.extractFiltered(annotation, namedPhrases)
        lst += ListMap("sentence_id" -> i, "entities" -> namedPhrases, "triples" -> openieTriples.map(tos))  
      }
    } 

    val finalresult = result + ("nlp_output" -> lst.toList)

    import org.json4s.JsonDSL._
    implicit val formats = DefaultFormats
    val d = Extraction.decompose(finalresult)
    val writer = new PrintWriter(new File("useRelation.json"))
    writer.write(pretty(render(d)))
    writer.close()
  }

  def encodeJson(src: AnyRef): JValue = {
    import org.json4s.{ Extraction, NoTypeHints }
    import org.json4s.JsonDSL.WithDouble._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    Extraction.decompose(src)
  }

  //def addressPaper(lines:String, path:String) {
  def addressPaper(lines:String, inPath:String) {
    val paperData = JSON.parseFull(lines).get.asInstanceOf[Map[String, Any]]("paper_content").asInstanceOf[List[Map[String,List[Map[String, String]]]]]
    var result : Map[String, List[ListMap[String, _]]] = Map()
    var convertedList = new ListBuffer[ListMap[String, _]]()
    for (section <- paperData) {
      for ((sectionName, sectionSegment) <- section) {
        if (sectionName != "References") {
          val templst = new ListBuffer[ListMap[String, _]]()
          for (sec <- sectionSegment) {
            for ((k, v) <- sec) {
              val annotation = getAnnotation(v)
              val namedPhrases = NamedPhraseExtractor.fancyextract(annotation)
              val openieTriples = OpenIEExtractor.extractFiltered(annotation, namedPhrases)
              templst += ListMap("Segment" -> k, "Entities"-> namedPhrases, "Triples" ->openieTriples.map(tos)) 
            }
          }
          convertedList += ListMap(sectionName -> templst.toList) 
        } 
      }
    }
    result += ("paper_content" -> convertedList.toList)
    
    import org.json4s.JsonDSL._
    implicit val formats = DefaultFormats
    val d = Extraction.decompose(result)
    //val writer = new PrintWriter(new File(path + "/"+ "parsed.json"))
    val paperName = inPath.split('/').last.split('.').head
    val resultName = inPath.split('/').dropRight(1).mkString("/") + "/" + paperName + "parsed.json" 
    val writer = new PrintWriter(new File(resultName))
    writer.write(pretty(render(d)))
    writer.close()
  
  }
  
  def getPaperJson(dirName : String) : Array[String] = {
    return (new File(dirName)).listFiles.filter(_.getName.endsWith(".json")).map(_.getName)
  }

  def getListOfSubDirectories(directoryName : String): Array[String] = {
    return (new File(directoryName)).listFiles.filter(_.isDirectory).map(_.getName)
  
  }

  def main(args: Array[String]) = {
    if (args.size == 0) {
      println("Missing argument = text-document-to-extract-triples")
      System.exit(1)
    }
    val inPath = args(0)
    //val lines = scala.io.Source.fromFile(inPath).getLines.toList.filter(_.size != 0)
    //val fileName = inPath.split('/').last
    val lines = scala.io.Source.fromFile(inPath).getLines.mkString

    addressPaper(lines, inPath)
   /*
   for (subdir <- getListOfSubDirectories(inPath)) {
     val paperPath = inPath + "/" + subdir 
     for (paper <- getPaperJson(paperPath)) {
       val lines = scala.io.Source.fromFile(paperPath + "/" + paper).getLines.mkString
       addressPaper(lines, paperPath)
     } 
   }
   */
  }
}
