package gov.pnnl.aristotle.utils

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import scala.collection.Set
import scala.collection.immutable.TreeSet
import scala.xml.XML
import gov.pnnl.fcsd.datasciences.graphBuilder.nlp.semanticParsers.SennaSemanticParser
import scala.Array.canBuildFrom

object MatchStringCandidates {

  //given a graph of type [String, String] ,  return list of vertex ids containing given label
 def getMatches(label :String, g :Graph[String, String]): Array[Long] = {
   val matchingVertices =  g.vertices.filter(v => v._2.toLowerCase().contains(label.toLowerCase())).map(v => v._1).collect()   
    if(matchingVertices.size == 0){
     if(label.contains(" ")){
       val pos = label.lastIndexOf(' ', label.length())
       val labelPrefix = label.substring(0, pos)
       return g.vertices.filter(v => v._2.toLowerCase().contains(labelPrefix.toLowerCase())).map(v => v._1).collect()
     }
   }
   return matchingVertices
   //return matchingVertices
}
 
  def getMatchesRDDWithAlias(mentions :List[String],  g :Graph[String, String]):RDD[(VertexId, String)]  = {
    val verticesWithLabels: VertexRDD[String] = NodeProp.getNodeAlias(g)
    val newGraph = g.joinVertices(verticesWithLabels)((id, a,b) => a+";"+b)
    
    val matchingVertices: RDD[(VertexId, String)] =  newGraph.vertices.filter(v => {
     var found = false;
     for(mention <- mentions) {
       if(v._2.toLowerCase().contains(mention.toLowerCase) || mention.toLowerCase().contains(v._2.toLowerCase()))
         found=true
     }
     found
   })
 
   return matchingVertices
 }
 //given a graph of type [String, String] ,  return RDD of nodes containing given label
 def getMatchesRDD(label :String, g :Graph[String, String]): RDD[(VertexId, String)] = {
   val matchingVertices = g.vertices.filter(v => v._2.toLowerCase().contains(label.toLowerCase()))
   if(matchingVertices.count == 0){
     if(label.contains(" ")){
       val pos = label.lastIndexOf(' ', label.length())
       val labelPrefix = label.substring(0, pos)
       return g.vertices.filter(v => v._2.toLowerCase().contains(labelPrefix.toLowerCase()))
     }
   }
   return matchingVertices
 }
}

class SennaConfig(filename: String){
   
   val allvars = XML.loadFile(filename)
   val sennaHome = (allvars \\ "SENNAHOME").text //"C:\\Users\\d3x771\\Desktop\\projs\\knowledge_graph\\aristotle-dev\\senna\\"
   val sennaExe = (allvars \\ "SENNAEXE").text  //"senna.exe"
   val configPath = (allvars \\ "CONFIGPATH").text //"C:\\Users\\d3x771\\Desktop\\projs\\knowledge_graph\\CONFIG.config"
   val scrapFolder = (allvars \\ "SCRAP").text //"C:\\Users\\d3x771\\Desktop\\projs\\knowledge_graph\\"
}

object Context{
 
  // Note once we have Vivek's library we only return list of identifiable objects in the input String
  def getObjectList(text: String, sennaConfig: SennaConfig): Set[String] = {
    //val sennaConfig = new SennaConfig(sennaConfigFile) 
    
    val args : Array[String]  = Array("-i", text, "-sennaHome", sennaConfig.sennaHome,
      "-sennaExecutable", sennaConfig.sennaExe , "-scrap", sennaConfig.scrapFolder,  "-config", sennaConfig.configPath)
    println("Callign Senna Semnatic parser  \n ") //+args.mkString(","))
    //print(text)
    val sennaOutput : String =  SennaSemanticParser.getSemanticRoleInfo(args)
    
    val allTriples: Array[String] = sennaOutput.replaceAllLiterally("-", "").replaceAllLiterally(">", "") .split('\n').filterNot(
        s => s.contains("NONE") || s.contains("'")).filter(_.length() > 4 )
    
    println("TRIPLES  RECIEVED")
    //allTriples.foreach(println(_))
    val ContextEdges:  Array[Array[String]] = allTriples.map(triple => {
      val pos1 = triple.indexOf('[')
      val pos2 = triple.indexOf(']')
      if(pos1 < 0 && pos2 < 0) println("Unformatted triple ", triple)
      Array(triple.substring(0, pos1),triple.substring(pos1+1, pos2), triple.substring(pos2+1) )      
    } )
    
    //print("triples: after ")
    //ContextEdges.foreach(t => t.foreach(println(_)))

    val setObjects: Set[String] = tripleToObjects(ContextEdges)   
    return setObjects  
  }
  
  def tripleToObjects(triples : Array[Array[String]]): Set[String] = {
  
    val objectTypes : Array[String] = Array("LOC", "PER", "ORG", "MISC" ) 
    var objects : Set[String] = Set()
    for ( t <- triples ) {
       if(objectTypes.contains(t(2).trim())) objects += t(0) 
    }
    print("genereated objects list:")
    //objects.foreach(println(_))
    return objects
  }
  
  //This can be implemented in a lot of smart ways , for now we have #word_matches based similarity
  def phraseSimilarity(phrase1: String , phrase2 : String) : Double = {
    
    //If same phrases
    if(phrase1.toLowerCase() == phrase2.toLowerCase()) return 1.0
    
    val splitters : Array[Char] = Array(' ', '_', ',', '$')
    // get a score for Partial match
    val wordsPhrase1 : Array[String] = phrase1.toLowerCase().split(splitters)
    val numWords1 = wordsPhrase1.length
    val wordsPhrase2 : Array[String] = phrase2.toLowerCase().split(splitters)
    val numWords2 = wordsPhrase2.length
    
    var score = 0.0
    // Note for every match , there are 2 words that match, so we increment score by 2
    //e.g steve_jobs and jobs_steve should match completely and get a score of 1.0
    // while Apple_Inc and Apple_Fruit will match by 2/4 = .5
    // We can add lot of heuristics here like returning a match when both words are used in same context 
    // e.g murder is equivalent to kill
    // Google (noun) != google (verb),  we can identify context from our NLP parser
    wordsPhrase1.foreach(word1 => 
      if (wordsPhrase2.contains(word1)) {score +=  2.0 
            
}        
    )
    score = score/(numWords1 + numWords2)
    return score
  }
  
  //Returns Similarity score b/w 2 sets containing keyphrases 
  // Similarity score is calculated as  cumulative similarity score b/w all possible combination of phrases
  def simScore(mentionContext : Set[String], candidateContext : Set[String]): Double = {
    var totalSimScore = 0.0
    var phraseSimScore = 0.0
    
    // We only consider a phrase match if at least 60% of phrase content is same
    val threshold = 0.5
    // For all words in mention Context 
    for (v1 <- mentionContext) {
      for( v2 <- candidateContext) {
        phraseSimScore = phraseSimilarity(v1,v2)   
        if( phraseSimScore >= threshold) { 
          //println("Phrase matched :" + v1 + "," + v2 )
          totalSimScore += phraseSimScore 
          }
      }
    }
   
    //Note this is NOT a normalized score, 
    // Intuition : An entity with a bigger neighbourhood will have lot more context 
    return (totalSimScore/(mentionContext.size*1.0))
    
  }
}


/*
class NerEdge(s: String, d : String, p : String){
  var src: String =s;
  var dest: String =d;
  var pred: String =p;
}
*/
