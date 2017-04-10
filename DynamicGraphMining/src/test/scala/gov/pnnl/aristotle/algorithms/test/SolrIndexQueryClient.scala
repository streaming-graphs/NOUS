package gov.pnnl.aristotle.algorithms.test

import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.solr.client.solrj.SolrQuery

object SolrIndexQueryClient {
  val urlString = "http://localhost:8983/solr/aristotle0";
  val solr = new HttpSolrServer(urlString);

  def main(args: Array[String]): Unit = {}
  val t0 = System.currentTimeMillis();
  // query
  val parameters = new SolrQuery();
  val mQueryString = "acronym:*HIMP*";
  parameters.set("q", mQueryString);

  val Qresponse = solr.query(parameters);
  val t1 = System.currentTimeMillis();
  println("Time to Query is(in seconds): " + (t1 - t0) / 1000);
  
  val list = Qresponse.getResults();
  //val i = 0;
  for (i <- 0 to list.size() - 1) {
    System.out.println(list.get(i));
  }
  
  
}