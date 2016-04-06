package gov.pnnl.aristotle.aiminterface.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import gov.pnnl.aim.af.client.ws.AimAfWsClientFactory
import org.apache.spark.graphx.Graph
import java.io.PrintWriter
import gov.pnnl.aristotle.algorithms.GraphProfiling
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import java.io.File
import java.io.InputStream
import org.apache.commons.io.IOUtils
import gov.pnnl.aristotle.utils.NodeProp
import gov.pnnl.aristotle.algorithms.PathSearch
import gov.pnnl.aristotle.aiminterface.NousAnswerStreamRecord
import gov.pnnl.aristotle.aiminterface.ShyrePiersLOBMessage
import collection.JavaConversions._
import gov.pnnl.aristotle.aiminterface.NousProfileAnswerStreamRecord
import gov.pnnl.aristotle.algorithms.InfoBox
import gov.pnnl.aristotle.algorithms.InfoBox
import gov.pnnl.aristotle.utils.NodeProp
object NOUS_Streaming_Driver {

  val sparkConf = new SparkConf().setAppName("Driver").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val shyreNousTopicName :String = "aim-piers-shyre-nous";
    val nousSOIStreamTopicName : String  = "aim-shyre-nous-soi";

    	val streamWS = AimAfWsClientFactory.generateAimAfStreamClientV2("https","aim-af",8443,"uchd_user","uchd_password");
    val lobDictionary = Map("AUTOMOTIVE" -> "wikicategory_car_manufacturers",
        "CLOTHING" -> "wikicategory_clothing_companies",
        "APPLIANCES" -> "wikicategory_home_appliance_manufacturers",
        "ATHLETIC STORES"-> "wikicategory_sporting_goods_manufacturers",
        "SUPERSTORES"->"	wikicategory_superstores_companies",
        "DEPARTMENT STORES"-> "wikicategory_department_stores_company"
)
        
  def main(args: Array[String]): Unit = {
    
   if(streamWS == null) 
    {
     println("***NO aim client*****");
     java.lang.System.exit(1)
    }
   

    if(args.length < 1) println("Not enough arguments, Usage:<pathToGraphFile(s)>")
    
    val graphFilename :String = args(0)
    println("Reading graph")
    val g : Graph[String, String] =  ReadHugeGraph.getGraph(graphFilename, sc)
    val writerSG = new PrintWriter(new File("driver.out"))
    val augGraph : Graph[(String, Map[String, Map[String, Int]]), String]= 
      GraphProfiling.getAugmentedGraph(g, writerSG)
    var mode = "listen"
    var counter=0;

   //read path question topic
   while(1==1)
   {
     var response = streamWS.retrieveDynamicTopicRecord(shyreNousTopicName, true);
   println("============Status Retrieve From Topic = " + 
       shyreNousTopicName + response.getStatus());
   val inPQ = response.getEntity().asInstanceOf[InputStream];
   val resultBytesPQ = IOUtils.toByteArray(inPQ);
   if(resultBytesPQ.length > 0)
   {
     println("got something . conf dir is" + args(1))
     counter = counter +1
     processShyreMessage(g,sc,resultBytesPQ,streamWS,
         nousSOIStreamTopicName,counter,writerSG,args(1))
   }
   }
    
  }
  
def processShyreMessage(g : Graph[String, String],sc:SparkContext,
     resultBytesPQ : Array[Byte], streamWS: gov.pnnl.aim.af.sei.v2.StreamSEI,
     nousSOIStreamTopicName : String,counter :Int,writerSG:java.io.PrintWriter,
     confDir: String)
 {
  //val confDir = "/pic/projects/nous/AIM-Demo/dint_conf/"
	System.out.println("got something from shyre queue answer");
	// Create POJO from Avro byte[]
	
	var message = new ShyrePiersLOBMessage();
	message = AimAfWsClientFactory.deserializeAvroRecordFromByteArray(resultBytesPQ, message.getClass());
    // Step 1 .Get Infobox Text
    // val companyID = SomeClass.getYagoIDForString(message.getCompany())
	//val confDir = ""
	val predMapFile = ""
    val infoboxText = InfoBox.getInfoBox(message.getCompany(), confDir,  g, sc)
    //val infoboxText: java.lang.String = "Toyota is known for Automotive_industry.\n Toyota makes Automobils, luxury vehicles,commercial vehicles, engines,motercycles.\n Toyota has homepage http://www.toyota-global.com \n "
      
	//Get Profile Summary of following things
	// 1. LOB profile 
	val typemap = GraphProfiling.getAugmentedTypeMap(g, writerSG)
	val augGraph = GraphProfiling.getAugmentedGraph(g, writerSG)
	//	val lobTypeInYago = SomeClass.getYagoIDForString(message.getLob().toString())
	var LOBProfile :NousProfileAnswerStreamRecord = 
	  GraphProfiling.getNodeTypeProfileObject(
	    typemap._1, lobDictionary.getOrElse(message.getLob(),"wikicategory_clothing_company"),"OutboundPredicateObjType")
	// 2. Company Profile
	val companyProfile : NousProfileAnswerStreamRecord = GraphProfiling.getNodeProfile(augGraph,
	    message.getCompany())
	//Combined both answers
	 var profileTriples = LOBProfile.getProfile() ++ companyProfile.getProfile()
	 val prettyTriples :  java.util.List[java.lang.String]= GraphProfiling.prettyProfile(profileTriples) 
	
	 
	 
	 //Get Path Results in Text Form 
	 val nbrList = NodeProp.getOneHopNbrIdsLabels(g)
	 
	// val itemList = SomeClass.getItemListFromHSCODE(message.getHscodes())
	 val hscodeFilePath="dint_conf/HSCODES.csv"
	  
	val pathText = PathSearch.FindPathsJavaList(message.getLob(), message.getHscodes(),confDir,g,nbrList, sc)  
	val pathTextList :java.util.List[java.lang.String] = List(pathText)
	//	  var pathText :java.util.List[java.lang.String] =  
//	    List("Shirt is a product of Perry_Ellis_International",
//	        "Perry_Ellis_International is a type of Clothing_companies_of_the_United_States")
//	    
	 // Create Combined Answer to SOI topic.   
	 var nousAnswer :NousAnswerStreamRecord = new NousAnswerStreamRecord(); 
	nousAnswer.setUuid("uuid")
	nousAnswer.setCompany(message.getCompany())
	nousAnswer.setProbability(message.getProbability())
	nousAnswer.setExamplarCompany(message.getExamplarCompany())
	nousAnswer.setExpectedCount(message.getExpectedCount())
	nousAnswer.setIsHighScore(message.getIsHighScore())
	nousAnswer.setHscodes(message.getHscodes())
	nousAnswer.setHighScore(message.getHighScore())
	nousAnswer.setLob(message.getLob())
	nousAnswer.setPaths(pathTextList)
	nousAnswer.setProfile(prettyTriples)
	nousAnswer.setInfobox(infoboxText)
	nousAnswer.setScore(message.getScore())
	nousAnswer.setRecordids(message.getRecordids())
	
	val  byteBuffer = AimAfWsClientFactory.serializeAvroRecordToByteBuffer(nousAnswer)
	val bytes = byteBuffer.array()

	val responsePQ = streamWS.submitDynamicTopicRecord(nousSOIStreamTopicName,bytes);
	println("published answer to soi Stream topic: " +counter);
	println("********************PROBABILITY: " + message.getProbability() 
	    + " " + nousAnswer.getProbability())
	println("********************LOB: " + message.getLob() 
	    + " " + nousAnswer.getLob())
	println("********************SCORE: " + message.getScore() 
	    + " " + nousAnswer.getScore())
	println("********************HIGH_SCORE: " + message.getHighScore() 
	    + " " + nousAnswer.getHighScore())

	println("********************IS_HIGHSCORE: " + message.getIsHighScore()
	    + " " + nousAnswer.getIsHighScore())
	println("********************HSCODE: " + message.getHscodes() 
	    + " " + nousAnswer.getIsHighScore())
	println("********************RECORDID: " + message.getRecordids() 
	    + " " + nousAnswer.getRecordids())
		println("********************Profile: " + nousAnswer.getProfile().toString())
	 	 	println("********************Path: " + nousAnswer.getPaths())
	 	 	println("********************Infobox: " + nousAnswer.getInfobox())
//	println("********************PROBABILITY: " + message.getProbability() 
//	    + " " + nousAnswer.getProbability())

 } 


}