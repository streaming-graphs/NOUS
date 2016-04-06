package gov.pnnl.aristotle.aiminterface.test;

import static gov.pnnl.aim.af.client.ws.AimAfWsClientFactory.AVRO_MAPPER;

import java.io.InputStream;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import gov.pnnl.aim.af.client.ws.AimAfWsClientFactory;
import gov.pnnl.aim.af.sei.v2.StreamSEI;
import gov.pnnl.aristotle.aiminterface.NousPathQuestionStreamRecord;

public class AIMInterfacetestJava {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		final long timestamp = System.currentTimeMillis();
		final StreamSEI streamWS = AimAfWsClientFactory
				.generateAimAfStreamClientV2("https","aim-af",8443,"uchd_user","uchd_password");
		final NousPathQuestionStreamRecord searchQuestion = new NousPathQuestionStreamRecord();
		//searchQuestion.setTimestemp(timestamp);
		searchQuestion.setUuid("uuid");
		searchQuestion.setSource("Gap");
		searchQuestion.setDestination("New York");
		searchQuestion.setMaxpathsize(5);
		// Create Avro byte[] from existing POJO

		ObjectWriter tm = AVRO_MAPPER.writerFor(NousPathQuestionStreamRecord.class);
		final byte[] bytes = AVRO_MAPPER
				.writerFor(NousPathQuestionStreamRecord.class)
				.with(AVRO_MAPPER
						.schemaFor(NousPathQuestionStreamRecord.class))
				.writeValueAsBytes(searchQuestion);
	//streamWS.
		System.out.println("DOne");
		// Create new dynamic Topic
		Response response = null;
		String topicName = "noussoi";
		response = streamWS.createDynamicTopic(topicName);

		// Send a message into this new dynamic topic
		response = streamWS.submitDynamicTopicRecord(topicName, bytes);
		// assertEquals(200, response.getStatus());

		response = streamWS.retrieveDynamicTopicRecord(topicName, true);
		// assertEquals(200, response.getStatus());

		final InputStream in = (InputStream) response.getEntity();
		final byte[] resultBytes = IOUtils.toByteArray(in);

		// Create POJO from Avro byte[]
		final NousPathQuestionStreamRecord output = AVRO_MAPPER
				.reader(NousPathQuestionStreamRecord.class)
				.with(AVRO_MAPPER
						.schemaFor(NousPathQuestionStreamRecord.class))
				.readValue(resultBytes);
		System.out.println(output.getSource());
		System.out.println(output.getDestination());
		//System.out.println(output.getTimestemp());

	}

}
