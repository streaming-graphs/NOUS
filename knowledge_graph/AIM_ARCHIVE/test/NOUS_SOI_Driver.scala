package gov.pnnl.aristotle.aiminterface.test

import java.io.File
import java.io.InputStream
import java.io.PrintWriter
import org.apache.commons.io.IOUtils
import org.apache.spark._
import org.apache.spark.graphx._
import gov.pnnl.aim.af.avro.WelcomeStreamRecord
import gov.pnnl.aim.af.client.ws.AimAfWsClientFactory
import gov.pnnl.aristotle.aiminterface.NousPathQuestionStreamRecord
import gov.pnnl.aristotle.aiminterface.NousProfileAnswerStreamRecord
import gov.pnnl.aristotle.aiminterface.NousProfileQuestionStreamRecord
import gov.pnnl.aristotle.algorithms._
import collection.JavaConversions._

object NOUS_SOI_Driver {
        
 def main(args: Array[String]): Unit = {
    
    
    val sparkConf = new SparkConf().setAppName("Driver").setMaster("local")
    val sc = new SparkContext(sparkConf)
    	
    val pathQuestionTopicName :String = "nous-path-question";
	val pathAnswerTopicName : String  = "nous-path-answer";
    val profileQuestionTopicName :String = "nous-profile-question-dev1";
	val profileAnswerTopicName : String  = "nous-profile-answer-dev1";
    
	
    val streamWS = AimAfWsClientFactory.generateAimAfStreamClientV2("https","aim-af",8443,"uchd_user","uchd_password");
    System.out.println("****************************"+new WelcomeStreamRecord().getMessage());
    
    
   if(streamWS == null) 
    {java.lang.System.exit(1)
     println("***NO aim client*****");
    }
   
    val npq = streamWS.createDynamicTopic(pathQuestionTopicName);
    println("============Status Create Topic nous-path-question ========== "+npq.getStatus());
  
    val npa = streamWS.createDynamicTopic(pathAnswerTopicName);
    println("============Status Create Topic nous-path-answer ========== "+npq.getStatus());
    
  
    if(args.length < 1) println("Not enough arguments, Usage:<pathToGraphFile(s)>")
    
    val graphFilename :String = args(0)
    println("Reading graph")
    val g : Graph[String, String] =  ReadHugeGraph.getGraph(graphFilename, sc)
    val writerSG = new PrintWriter(new File("driver.out"))
    val augGraph = GraphProfiling.getAugmentedGraph(g, writerSG)
    var mode = "listen"
      var counter=0;
    while(1 == 1){
	  //read path question topic
	   var responsePQ=  streamWS.retrieveDynamicTopicRecord(pathQuestionTopicName, true);
	   //println("============Status Retrieve From Topic = "+responsePQ.getStatus());
	   val inPQ = responsePQ.getEntity().asInstanceOf[InputStream];
	   val  resultBytesPQ = IOUtils.toByteArray(inPQ);
	   if(resultBytesPQ.length != 0)
	   {
	     println("got something")
	     counter = counter + 1;
    		// Create POJO from Avro byte[]
		var  question : NousPathQuestionStreamRecord = new NousPathQuestionStreamRecord();
		question.setUuid("1")
		try{
		  question  = AimAfWsClientFactory.deserializeAvroRecordFromByteArray(resultBytesPQ, question.getClass())
		}catch
		{
		  case e:Exception => println(e.printStackTrace()) 
		}
		 val entityLabel1 = question.getSource()
         val entityLabel2 = question.getDestination()
         val entityLabels = Array(entityLabel1, entityLabel2) 
         println("Asking path question about : "+entityLabel1 + " and " + entityLabel2)
	     val answer = PathSearch.FindPathsNousRecord(entityLabels, g,sc)
	     if(answer.getPaths().size < 1) 
	     {
	    	 	var dummyRes : java.util.List[java.lang.String] = List("no-search-path-found")
	       answer.setPaths(dummyRes)
	     }  
	     println("Found some answer");
	     println("Answering path question about : "+entityLabel1 + " and " + entityLabel2)
	     
	     val  byteBuffer = AimAfWsClientFactory.serializeAvroRecordToByteBuffer(answer)
	     val bytes = byteBuffer.array()
	     
		responsePQ = streamWS.submitDynamicTopicRecord(pathAnswerTopicName,bytes);
		println("published answer to path topic: " +counter);
		
	     
	   }
	   
	   
	   // read profile qustion
	   var responseProfileQ=  streamWS.retrieveDynamicTopicRecord(profileQuestionTopicName, true);
	   //println("============Status Retrieve From Topic = "+responseProfileQ.getStatus());
	   val inProfileQ = responseProfileQ.getEntity().asInstanceOf[InputStream];
	   val  resultBytesProQ = IOUtils.toByteArray(inProfileQ);
	   if(resultBytesProQ.length != 0)
	   {
	     counter = counter + 1;
    		// Create POJO from Avro byte[]
		var  profilequestion : NousProfileQuestionStreamRecord = new NousProfileQuestionStreamRecord();
		profilequestion.setUuid("1")
		
		profilequestion  = AimAfWsClientFactory.deserializeAvroRecordFromByteArray(resultBytesProQ, profilequestion.getClass())
	     val entityLabel1 = profilequestion.getSource()
	     val uuid = profilequestion.getUuid()
	     
	     System.err.println("**************Got Profile question for "+entityLabel1)
        
      
	     var answer = GraphProfiling.getNodeProfile(augGraph, entityLabel1)
	     if(answer == null)
	     {
	       answer = new NousProfileAnswerStreamRecord();
	     }
	     if(answer.getProfile().size < 1) 
	     {
	    	 	var dummyRes : java.util.List[java.lang.String] = List("no-profile-value-found")
	       answer.setProfile(dummyRes)
	     }  
	     println("Found some answer from profile");
	     // Create Avro byte[] from existing POJO
	     answer.setUuid("1")
	     System.err.println("Publishing to profile queue "+profileAnswerTopicName) 
	     
	     val byteBuffer = AimAfWsClientFactory.serializeAvroRecordToByteBuffer(answer)
	     val bytes = byteBuffer.array()
	     
	     
		responseProfileQ = streamWS.submitDynamicTopicRecord(profileAnswerTopicName,bytes);
		println("published answer topic : " + counter);
	   }
  }
  println("Exiting..")
}
}