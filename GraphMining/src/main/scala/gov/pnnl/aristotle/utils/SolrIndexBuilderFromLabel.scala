package gov.pnnl.aristotle.utils
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.Graph
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.spark.graphx.{ VertexRDD, VertexId }
import java.util.ArrayList
import collection.JavaConversions._


object SolrIndexBuilderFromLabel {

  val filename = "/sumitData/myprojects/AIM/aristotle-dev/knowledge_graph/yagowikiinfoFullApple.ttl"
  val urlString = "http://localhost:8983/solr/aristotle0";
  val solr = new HttpSolrServer(urlString);
  //val sparkConf = new SparkConf().setAppName("SOLR Index Builder").setMaster("local")
  //val sc = new SparkContext(sparkConf)
  
 
  def getIndexBoW(f: (VertexId,String)) : Array[String]= {

    // token is bag of words
    return f._2.split(" ");
   
 }    
 
 def getIndexAcronym(f : (VertexId,String) ) : String= {
   
      // token is bag of words
      val alltoken = f._2.split(" ");
      // acronym is a upper case string of all the characters at place 0 of all the tokens
      val acronym= new StringBuffer();
      alltoken.foreach(f => {acronym.append(f.toString.trim().charAt(0).toUpper)})
      return acronym.toString();

 }    
 
 def indexGraph(graph: Graph[String, String],filename:String) :Unit = {
    val t00 = System.currentTimeMillis();
    val av: VertexRDD[String] = graph.vertices;
    //val docArray: ArrayBuffer[SolrInputDocument] = new ArrayBuffer[SolrInputDocument];
    //java.util.Collection<SolrInputDocument> docArray = new ArrayList<SolrInputDocument>();
     var docArray = new ArrayList[SolrInputDocument](); 
    av.foreach(f => {
      val document = new SolrInputDocument();
      document.addField("id", f._1);

      val alltoken = getIndexBoW(f);
      document.addField("token", alltoken)

      val acronym = getIndexAcronym(f);
      document.addField("acronym", acronym);

      docArray.append(document)
      
    })
    //create a batch and then commit
    println("Creating Set from docArray: ");
    val response = solr.add(docArray);
    val t100 = System.currentTimeMillis();
    println("Time Before Starting commit is(in seconds): " + (t100 - t00) / 1000);
    println("Starting commit: ");
    solr.commit();

    val t10 = System.currentTimeMillis();
    println("Time to index is(in seconds): " + (t10 - t00) / 1000);
 }
 
 
 
 /*fullAppl took Time to index is(in seconds): 43 to index in aristotl0
 // Time to index is(in seconds): 41 in tt
 If I pass graph as varibale to 	indexGraph(graph, filename, sc, solr)
 Exception in thread "main" org.apache.spark.SparkException: Task not serializable
 ...
 at gov.pnnl.aristotle.indexing.SolrIndexBuilderFromLabel$.indexGraph(SolrIndexBuilderFromLabel.scala:41)
	at gov.pnnl.aristotle.indexing.SolrIndexBuilderFromLabel$.main(SolrIndexBuilderFromLabel.scala:64)
	at gov.pnnl.aristotle.indexing.SolrIndexBuilderFromLabel.main(SolrIndexBuilderFromLabel.scala)
 * 
 * */
 def main(args: Array[String]): Unit = {

    //val graph: Graph[String, String] = ReadHugeGraph.getGraph(filename,sc)
  	//indexGraph(graph, filename)


 

  }

}