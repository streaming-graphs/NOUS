package gov.pnnl.aristotle.aiminterface.test
import gov.pnnl.aim.af.client.ws.AimAfWsClientFactory.AVRO_MAPPER
import gov.pnnl.aim.af.client.ws.AimAfWsClientFactory
import com.fasterxml.jackson.databind.ObjectWriter
import java.io.InputStream
import org.apache.commons.io.IOUtils
import com.fasterxml.jackson.databind.ObjectReader
import gov.pnnl.aristotle.aiminterface.NousPathQuestionStreamRecord
import gov.pnnl.aristotle.aiminterface.NousPathQuestionStreamRecord

object AIMInterfaceTest {

  
  def main(args: Array[String]): Unit = {
    val timestamp = System.currentTimeMillis();
    val streamWS = AimAfWsClientFactory.generateAimAfStreamClientV2("https","aim-af",8443,"uchd_user","uchd_password");
    val searchQuestion = new NousPathQuestionStreamRecord();
    //searchQuestion.setTimestemp(timestamp);
    searchQuestion.setUuid("uuid")
    searchQuestion.setSource("Gap2")
    searchQuestion.setDestination("New York")
    searchQuestion.setMaxpathsize(5)
    // Create Avro byte[] from existing POJO
     
    val  bytes = AVRO_MAPPER	.writerFor(searchQuestion.getClass()).asInstanceOf[ObjectWriter].`with`(AVRO_MAPPER.schemaFor(searchQuestion.getClass()))	.writeValueAsBytes(searchQuestion);
    // Create new dynamic Topic
		//var response = null;
		val topicName = "noussoi-scal";
		var response = streamWS.createDynamicTopic(topicName);

		// Send a message into this new dynamic topic
		response = streamWS.submitDynamicTopicRecord(topicName, bytes);
		// assertEquals(200, response.getStatus());

		response = streamWS.retrieveDynamicTopicRecord(topicName, true);
		// assertEquals(200, response.getStatus());

		val in =  response.getEntity().asInstanceOf[InputStream];
		val  resultBytes = IOUtils.toByteArray(in);

		// assertTrue(resultBytes.length > 0);

		// Create POJO from Avro byte[]
		var  output : NousPathQuestionStreamRecord = new NousPathQuestionStreamRecord();
		
		output  = AVRO_MAPPER
				.reader(output.getClass()).asInstanceOf[ObjectReader]
				.`with`(AVRO_MAPPER
						.schemaFor(output.getClass()))
				.readValue(resultBytes);
		System.out.println(output.getSource());
		System.out.println(output.getDestination());
		//System.out.println(output.getTimestemp());

  
  }

}