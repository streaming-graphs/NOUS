
package gov.pnnl.nous.pathSearch.Int

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import gov.pnnl.nous.utils.ReadGraph.{isValidLineFromGraphFile, getFieldsFromLine}
import PathSearchIntDataTypes.{VertexId, VertexEmb, IntEdge}

object DataReader {
 def getGraphInt(filename: String, sc: SparkContext, sep: String = "\t", lineLen : Int = 3): RDD[(VertexId, Iterable[IntEdge] )] = {
    println("starting map phase1");
    val triples: RDD[(VertexId, Iterable[IntEdge] )] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln))
      .map(line => getFieldsFromLine(line, sep)).filter(_.length == lineLen)
      .flatMap(fields => Array(
          (fields(0).toLong, (fields(2).toLong, fields(1).toInt)),
          (fields(2).toLong, (fields(0).toLong, fields(1).toInt))
          )).groupByKey
    triples.cache
  }
  
  def getTopics(filename: String, sc: SparkContext, vertexTopicSep : String = "\t", topicSep: String = ","): RDD[(VertexId, VertexEmb )] = {
    println("starting map phase1");
    val topics: RDD[(VertexId, VertexEmb)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln))
      .map(line => line.split(vertexTopicSep)).filter(_.length == 2)
      .map(idWithTopic => (idWithTopic(0).toLong, idWithTopic(1).split(topicSep).map(t => t.toDouble)))
        
    topics.cache
  }
}