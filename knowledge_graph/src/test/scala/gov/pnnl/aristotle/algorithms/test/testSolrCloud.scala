package gov.pnnl.aristotle.algorithms.test
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.SolrInputDocument;


object testSolrCloud {

  def main(args: Array[String]): Unit = {}

  
val server = new CloudSolrServer("localhost:9983");
server.setDefaultCollection("gettingstarted");
val doc = new SolrInputDocument();
doc.addField( "id", "1234");
doc.addField( "name", "A lovely summer holiday");
server.add(doc);
server.commit();
}