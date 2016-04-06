package gov.pnnl.aristotle.aiminterface.test;



import java.io.InputStream;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.A;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;

import gov.pnnl.aim.af.client.ws.AimAfWsClientFactory;
import gov.pnnl.aim.af.sei.v2.StreamSEI;
import gov.pnnl.aristotle.aiminterface.NousPathAnswerStreamRecord;
import gov.pnnl.aristotle.aiminterface.NousPathQuestionStreamRecord;
import gov.pnnl.aristotle.aiminterface.NousProfileAnswerStreamRecord;
import gov.pnnl.aristotle.aiminterface.NousProfileQuestionStreamRecord;
import static gov.pnnl.aim.af.client.ws.AimAfWsClientFactory.AVRO_MAPPER;

public class SOI_Client {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		String pathQuestionTopicName = "nous-path-question";
		String pathAnswerTopicName = "nous-path-answer";
		String profileQuestionTopicName  = "nous-profile-question-dev1";
		String profileAnswerTopicName   = "nous-profile-answer-dev1";
	    
		
		final long timestamp = System.currentTimeMillis();
		final StreamSEI streamWS = AimAfWsClientFactory
				.generateAimAfStreamClientV2("https","aim-af",8443,"uchd_user","uchd_password");
		
		
		//Construct a path question
		final NousPathQuestionStreamRecord pathQuestion = new NousPathQuestionStreamRecord();
		//pathQuestion.setTimestemp(timestamp);
		pathQuestion.setUuid("uuid");
		pathQuestion.setSource("honda");
		pathQuestion.setDestination("Steel");
		pathQuestion.setMaxpathsize(5);
		pathQuestion.setVersion(1);
		
		//Construct a profile question
		final NousProfileQuestionStreamRecord profileQuestion = new NousProfileQuestionStreamRecord();
		profileQuestion.setSource("chrysler");
		profileQuestion.setUuid("1");
		
		
		// Create Avro byte[] from existing POJO path question
		final byte[] bytes = AimAfWsClientFactory.serializeAvroRecordToByteBuffer(pathQuestion).array(); 

		Response response = null;
		// Send a message into this new dynamic topic
		response = streamWS.submitDynamicTopicRecord(pathQuestionTopicName, bytes);
		
		
		//create avro byte[] from profile question
		byte[] profilebytes = AimAfWsClientFactory.serializeAvroRecordToByteBuffer(profileQuestion).array();
		Response profileResponse = null;
		profileResponse = streamWS.submitDynamicTopicRecord(profileQuestionTopicName, profilebytes);
	
		
				
		while(1==1)
		{
			response = streamWS.retrieveDynamicTopicRecord(pathAnswerTopicName, true);
			profileResponse = streamWS.retrieveDynamicTopicRecord(profileAnswerTopicName, true);
			// assertEquals(200, response.getStatus());

			final InputStream in = (InputStream) response.getEntity();
			final byte[] resultBytes = IOUtils.toByteArray(in);

			final InputStream profin = (InputStream) profileResponse.getEntity();
			final byte[] profileResultBytes = IOUtils.toByteArray(profin);
			
			if(resultBytes.length > 0)
			{
				System.out.println("got something from path answer");
				// Create POJO from Avro byte[]
				
				NousPathAnswerStreamRecord answer = new NousPathAnswerStreamRecord();
				answer = AimAfWsClientFactory.deserializeAvroRecordFromByteArray(resultBytes, answer.getClass());
				System.out.println(answer.getSource());
				System.out.println(answer.getDestination());
				//System.out.println(answer.getTimestemp());
				if(answer.getPaths() != null)
					for(Object path:answer.getPaths())
						System.out.println(path.toString());
			}
			
			if(profileResultBytes.length > 0)
			{
				System.out.println("got something from profile answer");
				//val answer = new String()
				
				NousProfileAnswerStreamRecord answer=new NousProfileAnswerStreamRecord();
				
				 answer = AimAfWsClientFactory.deserializeAvroRecordFromByteArray(profileResultBytes, answer.getClass());
				System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$"+answer.getSource());
				if(answer.getProfile() != null)
					for(Object path:answer.getProfile())
						System.out.println(path.toString());
			}


		}
	}

}
