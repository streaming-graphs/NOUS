package gov.pnnl.aristotle.algorithms

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{ VertexRDD, VertexId }
import java.io.PrintWriter
import java.io.File
import akka.dispatch.Foreach
import org.apache.spark.graphx.GraphLoader
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.solr.common.SolrInputDocument
import java.io.IOException
import scala.collection.Set
import java.time.format.DateTimeFormatter
import java.util.Formatter.DateTime
import org.joda.time.format.DateTimeFormat
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import java.util.Calendar
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import org.apache.spark.graphx.PartitionStrategy

object ReadHugeGraph {

  //val sparkConf = new SparkConf().setAppName("Test Huge GraphX").setMaster("local")
  //val sc = new SparkContext(sparkConf)
  //val writerSG = new PrintWriter(new File("sampleGraph.ttl"))
  //val writerSGApple = new PrintWriter(new File("sampleGraphApple.ttl"))
  val edgeListFileName = "edgeList.txt"
  val edgeListFile = new PrintWriter(new File(edgeListFileName))
  //val filename = "/sumitData/myprojects/AIM/aristotle-dev/knowledge_graph/yagowikiinfo.ttl"
  
  def getEdgeListFile(filename : String, sc : SparkContext ) : Graph[Int, Int]= {
   println("In getEdgeList before map")
    val edges: RDD[Edge[String]] = sc.textFile(filename).map { line =>
        val fields = getFieldsFromLine(line)
        getLabelledEdge_FromTriple(fields)
      }
//   edges.collect.foreach(e =>{
//     edgeListFile.println(e.srcId + " " +e.dstId)
//   })
   edgeListFile.flush();
   println("In getEdgeList : building graph")
   val graph = GraphLoader.edgeListFile(sc, edgeListFileName, false, 1)
   println("edge count " + graph.edges.count)
   println("vertices count" + graph.vertices.count)
   return graph
  }
  
  def getFieldsFromLine(line :String) : Array[String] = {
    return line.toLowerCase().replaceAllLiterally("<", "").replaceAllLiterally(">", "").replace(" .", "").split("\\t").map(str => str.stripPrefix(" ").stripSuffix(" "));
  }
  
  //(radio frequency interference	resulted in	crash near mcg		radio frequency interference	likely resulted in	crash of aircraft	,64)
  def getFieldsFromPatternLine(line :String) : Array[String] = {
    val tmp =  line.toLowerCase().replaceAllLiterally("<", "").replaceAllLiterally(">", "")
    .replaceAllLiterally("(", "").replace(" .", "").replaceAllLiterally(")", "")
    val i = tmp.lastIndexOf(",")
    return Array(tmp.substring(0, i), tmp.substring(i+1))
    //split(",").map(str => str.stripPrefix(" ").stripSuffix(" ").replaceAll(",", ""));
  }
  
  def getFieldsFromPatternLine_BatchFormat(line :String) : Array[(String,List[(Int,Long)])] = {
    val tmp =  line.toLowerCase().replaceAllLiterally("<", "").replaceAllLiterally(">", "")
    .replaceAllLiterally("(", "").replace(" .", "").replaceAllLiterally(")", "")
    val i = tmp.lastIndexOf(",")
    val remaining_string = tmp.substring(i+1).trim()
    val batch_support_array_raw = remaining_string.split('|')
    val batched_support_array = batch_support_array_raw.map(f=> (f.split(" ")(0).toInt, f.split(" ")(1).toLong))
    return  Array((tmp.substring(0, i),batched_support_array.toList))
    //split(",").map(str => str.stripPrefix(" ").stripSuffix(" ").replaceAll(",", ""));
  }
  /*
   * person works_at        company         company makes   sup_mt5         
   * person  works_at        o5              o5      makes   sup_mt5         
   * person  works_at        o5              pe    rson  works_at        company
   * ,Set(otheruser0, user5)) 
   */
    def getFieldsFromPatternNodeLine(line :String) : Array[(String,(Int,List[String]))] = {
    val tmp =  line.toLowerCase().replaceAllLiterally("<", "").replaceAllLiterally(">", "")
    .replace(" .", "").replaceAllLiterally(")", "").replaceAllLiterally("(", "")
    
    val batch_support_array_raw = tmp.trim().split(",set")
    //val batched_support_array = batch_support_array_raw.map(f=> (f.split(" ")(0).toInt, f.split(" ")(1).toLong))
    val key = batch_support_array_raw(0)
    val value = batch_support_array_raw(1).split(",")
    return  Array((batch_support_array_raw(0),(value.length,value.toList)))
    //split(",").map(str => str.stripPrefix(" ").stripSuffix(" ").replaceAll(",", ""));
  }
  
  def getFieldsFromLineLG(line :String) : Array[String] = {
    return line.toLowerCase().replaceAllLiterally("<", "").replaceAllLiterally(">", "").replace(" .", "").split(" ").map(str => str.stripPrefix(" ").stripSuffix(" "));
  }
  def isValidLineFromGraphFile(ln : String) : Boolean ={
    ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false))
  }
  
  /**
   * getLabelledEdge_FromQuadruple
   */
  def getLabelledEdge_FromQuadruple(fields : Array[String]) : Edge[String] = {
    println(fields(3).toString())
    	return Edge(fields(1).hashCode().toLong, 
            fields(3).hashCode().toLong, fields(2))
  }
    def getTemporalLabelledEdge_FromQuadruple(fields : Array[String]) : Edge[KGEdge] = {
    if(fields(3).matches("^\\d{4}-\\d{2}-\\d{2}t.*$"))
    {      
    
//    val f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ");
//    val dateTime = f.parseDateTime(fields(3).replaceAll("t", " "));
    	val longtime = 1L//dateTime.getMillis()
      return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, new KGEdge(fields(1),longtime))
    }
    else if(fields(3).matches("^\\d{4}$"))
    {
      
            //println("found  edge "+fields(1))
            return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, new KGEdge(fields(1),-1L))
    }
    else  
    	return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, new KGEdge(fields(1),-1L))
  }
  
  def getPatternRDD_FromQuadruple(fields : Array[String]) : Edge[KGEdge] = {
    if(fields(3).matches("^\\d{4}-\\d{2}-\\d{2}t.*$"))
    {      
    
    val f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    val dateTime = f.parseDateTime(fields(3).replaceAll("t", " "));
    val longtime = dateTime.getMillis()
      return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, new KGEdge(fields(1),longtime))
    }
    else if(fields(3).matches("^\\d{4}$"))
    {
      
            //println("found  edge "+fields(1))
            return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, new KGEdge(fields(1),-1L))
    }
    else  
    	return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, new KGEdge(fields(1),-1L))
  }
    

  def getTemporalLabelledEdge_FromTriple(fields : Array[String]) : Edge[KGEdge] = {
    try{return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, new KGEdge(fields(1),-1L))
    } catch {
      case ex: java.lang.ArrayIndexOutOfBoundsException => {
                println(fields.toSeq.toList.toString)

            println("IO Exception" )
            return Edge("None".hashCode().toLong,
                "None".hashCode().toLong,
                new KGEdge("None",-1L))
         }
}        
  }    
    
  def getLabelledEdge_FromTriple(fields : Array[String]) : Edge[String] = {
    try{return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, fields(1))
    } catch {
      case ex: java.lang.ArrayIndexOutOfBoundsException => {
                println(fields.toSeq.toList.toString)

            println("IO Exception" )
            return Edge("None".hashCode().toLong,
                "None".hashCode().toLong,
                "None")
         }
}        
  }
  
  def getVertex_FromString(field : String) : (VertexId, String) = {
    try{ return (field.hashCode().toLong, field)
      
    }catch { 
      case ex: java.lang.ArrayIndexOutOfBoundsException => {
            println("Making Node :Array index Exception ")
            return ("None".hashCode().toLong,
                "None")
         }
    }  
  }
  
   def getTemporalGraph(filename : String, sc : SparkContext): Graph[String, KGEdge] = {
    println("starting map phase1");
    val quadruples: RDD[(String, String, String,String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        val fields = getFieldsFromLine(line);
        if (fields.length == 4)
          (fields(0), fields(1), fields(2),fields(3))
        else if(fields.length == 3)
          (fields(0), fields(1), fields(2),"1L")
        else {
          println("Exception reading graph file line", line)
          ("None", "None", "None","-1L")
        }
      }

    quadruples.cache
    val edges = quadruples.map(quadruple => {
      Edge(quadruple._1.hashCode().toLong, quadruple._3.hashCode().toLong, new KGEdge(quadruple._2, -1L))
    })
    val vertices = quadruples.flatMap(triple => Array((triple._1.hashCode().toLong, triple._1), (triple._3.hashCode().toLong, triple._3)))

    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
    
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
    
    return graph
  }
  
 def getTemporalGraphInt(filename : String, sc : SparkContext): Graph[Int, KGEdgeInt] = {
    println("starting map phase1");
    val quadruples: RDD[(Int, Int, Int,Long)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        var longtime = -1L
        val fields = getFieldsFromLine(line);
        try{
          val f = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS");
          val dateTime = f.parseDateTime(fields(3));
          longtime = dateTime.getMillis()
        }catch{
          case ex: org.joda.time.IllegalFieldValueException => {
          }
        }
        

        if (fields.length == 4)
          (fields(0).toInt, fields(1).toInt, fields(2).toInt,longtime)
        else if(fields.length == 3)
          (fields(0).toInt, fields(1).toInt, fields(2).toInt,0)
        else {
          println("Exception reading graph file line", line)
          (-1,-1,-1,-1)
        }
      }

    quadruples.cache
    val edges = quadruples.map(quadruple => {
      Edge(quadruple._1.toLong, quadruple._3.toLong, new KGEdgeInt(quadruple._2, quadruple._4))
    })
    val vertices = quadruples.flatMap(triple => Array((triple._1.toLong, triple._1), (triple._3.hashCode().toLong, triple._3)))

    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
    
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
    
    return graph.partitionBy(PartitionStrategy.EdgePartition2D)
  }
   
  def getGraph_old(filename : String, sc : SparkContext): Graph[String, String] = {
    println("starting map phase1");
    val edges_non_unique: RDD[Edge[String]] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        val fields = getFieldsFromLine(line);
        if (fields.length == 4)
          getLabelledEdge_FromQuadruple(fields)
        else
          getLabelledEdge_FromTriple(fields)
      }
    val edges = edges_non_unique.distinct
    //edges.cache()
    println("starting map phase2");
    val vertexRDD1: RDD[(VertexId, String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        val fields = getFieldsFromLine(line)
        if (fields.length == 4)
          getVertex_FromString(fields(1))
        else
          getVertex_FromString(fields(0))        
      }
    println("starting map phase3");
    //vertexRDD1.cache
    val vertexRDD2: RDD[(VertexId, String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        val fields = getFieldsFromLine(line)
        if (fields.length == 4)
          getVertex_FromString(fields(3))
        else
          try {
            getVertex_FromString(fields(2))
          }catch { 
      case ex: java.lang.ArrayIndexOutOfBoundsException => {
            println("Making Node :Array index Exception ")
                    println(fields.toSeq.toList.toString)

            ("None".hashCode().toLong,
                "None")
         }
          }
      }
    //vertexRDD2.cache
    println("starting map phase4 > doing union");
    val allvertex = vertexRDD1.union(vertexRDD2)

    println("starting map phase5 > Building graph");
    val graph = Graph(allvertex, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
  
    //graph.vertices.foreach(v => println(v._2))
    return graph
  }
  
  def getGraphTimeStamped(filename : String, sc : SparkContext): Graph[String, String] = {
    println("starting map phase1");
    val triples: RDD[(String, String, String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        val fields = getFieldsFromLine(line);
        if (fields.length == 4 || fields.length == 3)
          (fields(0), fields(1), fields(2))
        else {
          println("Exception reading graph file line", line)
          ("None", "None", "None")
        }
      }

    triples.cache
    val edges = triples.map(triple => Edge(triple._1.hashCode().toLong, triple._3.hashCode().toLong, triple._2))
    val vertices = triples.flatMap(triple => Array((triple._1.hashCode().toLong, triple._1), (triple._3.hashCode().toLong, triple._3)))

    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
 
    return graph
  }
  
  def getGraphTimeStampedLAS(filename : String, sc : SparkContext): Graph[String, String] = {
    println("starting map phase1");
    val triples: RDD[(String, String, String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).distinct.map { line =>
        val fields = getFieldsFromLine(line);
        if (fields.length == 4 || fields.length == 3){
          var subj = fields(0).replace(',', ';')
          val pred = fields(1).replace(',', ';')
          var objt = fields(2).replace(',', ';')
          if(subj == "drones")
            subj = "drone"
          if(objt == "drones")
            objt = "drone"
          
          (subj, pred, objt)
        } else {
          println("Exception reading graph file line", line)
          ("None", "None", "None")
        }
      }

    triples.cache
    val edges = triples.map(triple => Edge(triple._1.hashCode().toLong, triple._3.hashCode().toLong, triple._2))
    val vertices = triples.flatMap(triple => Array((triple._1.hashCode().toLong, triple._1), (triple._3.hashCode().toLong, triple._3)))

    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
 
    return graph
  }
 
  def getGraph(filename : String, sc : SparkContext): Graph[String, String] = {
    println("starting map phase1");
    val triples: RDD[(String, String, String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        val fields = getFieldsFromLine(line);
        if (fields.length == 4)
          (fields(1), fields(2), fields(3))
        else if(fields.length == 3)
          (fields(0), fields(1), fields(2))
        else {
          println("Exception reading graph file line", line)
          ("None", "None", "None")
        }
      }

    triples.cache
    val edges = triples.map(triple => Edge(triple._1.hashCode().toLong, triple._3.hashCode().toLong, triple._2))
    val vertices = triples.flatMap(triple => Array((triple._1.hashCode().toLong, triple._1), (triple._3.hashCode().toLong, triple._3)))

    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
 
    return graph
  }
  
  def isEdgeLineLG(ln : String): Boolean = {
    ln.startsWith("e ")
  }
  
  def isVertexLineLG(ln:String): Boolean = {
    ln.startsWith("v ")
  }
  
    def getGraphLG_Raw(filename : String, sc : SparkContext): Graph[String, String] = {
     println("starting map phase1");
     val edges_multiple: RDD[Edge[String]] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
      val fields: Array[String] = getFieldsFromLineLG(line);
      if(fields.length ==4 && isEdgeLineLG(line)) {
       Edge(fields(1).toLong, fields(2).toLong, "E")
      } 
//      else if(fields.length == 3 && isVertexLineLG(line)) {
//        Edge(fields(1).hashCode.toLong, fields(2).hashCode().toLong, "rdf:type")   
//      }
      else {
        println("Incorrect format", line)
        //exit
        Edge(-1,-1," ")
      }
     }
val edges = edges_multiple.distinct
    //edges.cache()
    println("starting map phase2");
    val vertices: RDD[(VertexId, String)] =
    sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln) && isVertexLineLG(ln)).flatMap { line =>
        val fields = getFieldsFromLineLG(line)
        if(fields.length != 3) {
          println("Incorrect graph format", line)
          exit
        }
        val node1 = getVertex_FromString(fields(1))
        val node2 = getVertex_FromString("Type_" + fields(2))
        
        Array(node1, node2)
        
    }
  
    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
  
    //graph.vertices.foreach(v => println(v._2))
    //graph.edges.foreach(println(_))
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
    return graph
  }
  
   /**
    * read elsevier file which is a tsv file. First column is paper ID, 
    * this should be tag with "rdf:type paper"
    */
    def getGraphElsevier_Temporal(filename : String, 
        sc : SparkContext): Graph[String, KGEdge] = {
     
      var non_type_graph = getTemporalGraph(filename, sc)
      val all_source_nodes = non_type_graph.triplets.map(triplets 
        => (triplets.srcId, triplets.srcAttr)).distinct
      val new_eges_to_show_vertexttype = all_source_nodes.map(v
          =>Edge(v._1, "paper".hashCode().toLong, new KGEdge("rdf:type",-1L)))
      val paper_nod_rdd = sc.parallelize(Array(getVertex_FromString("paper")))
      return Graph(non_type_graph.vertices.union(paper_nod_rdd),
          non_type_graph.edges.union(new_eges_to_show_vertexttype))
      
    }
    
    
    /**
    * @author puro755
    * read tsv file craeted by peng file which is a tsv file.  
    * 
    */
    def getGraphTSV_Temporal(filename : String, 
        sc : SparkContext): Graph[String, KGEdge] = {
     
      val non_type_graph = getTemporalGraph(filename, sc)
      return non_type_graph	
    }
    
    /*
     * Temporal version of getGraphLG
     */
    
  def getGraphLG_Temporal(filename : String, sc : SparkContext): Graph[String, KGEdge] = {
    println("starting map phase1");
    val triples: RDD[(String, String, String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        
        val fields = getFieldsFromLineLG(line);
        if (fields.length == 4 && isEdgeLineLG(line))
          (fields(1), "E", fields(2))
        else if(fields.length == 3 && isVertexLineLG(line))
          (fields(1), "rdf:type", fields(2))
        else {
          println("Exception reading graph file line", line)
          ("None", "None", "None")
        }
      }

    triples.cache
    val edges = triples.filter(e=>(e._2.equalsIgnoreCase("none")==false)).map(triple => Edge(triple._1.hashCode().toLong, triple._3.hashCode().toLong, new KGEdge(triple._2,-1L)))
    val vertices = triples.filter(v=>(v._2.equalsIgnoreCase("none")==false)).flatMap(triple => Array((triple._1.hashCode().toLong, triple._1), (triple._3.hashCode().toLong, triple._3)))

    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
 
    return graph
  }  
  
  
      
  def getGraphLG_TemporalInt(filename : String, sc : SparkContext): Graph[Int, KGEdgeInt] = {
    println("starting map phase1");
    val triples: RDD[(Int, Int, Int)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        
        val fields = getFieldsFromLineLG(line);
        if (fields.length == 4 && isEdgeLineLG(line))
          (fields(1).toInt, 11, fields(2).toInt)
        else if(fields.length == 3 && isVertexLineLG(line))
          (fields(1).toInt, 10, fields(2).toInt)
        else {
          println("Exception reading graph file line", line)
          (-1, -1, -1)
        }
      }

    triples.cache
    val edges = triples.filter(e=>(e._2!= -1)).map(triple => Edge(triple._1.toLong, triple._3.toLong, new KGEdgeInt(triple._2,-1L)))
    val vertices = triples.filter(v=>(v._2 != -1)).flatMap(triple => Array((triple._1.toLong, triple._1), (triple._3.toLong, triple._3)))

    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
 
    return graph
  }  
  
  def getGraphLG(filename : String, sc : SparkContext): Graph[String, String] = {
     println("starting map phase1");
     val edges: RDD[Edge[String]] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
      val fields: Array[String] = getFieldsFromLineLG(line);
      if(fields.length ==4 && isEdgeLineLG(line)) {
       Edge(fields(1).hashCode().toLong, fields(2).hashCode().toLong, "E")
      } 
      else if(fields.length == 3 && isVertexLineLG(line)) {
        Edge(fields(1).hashCode.toLong, fields(2).hashCode().toLong, "rdf:type")   
      }
      else {
        println("Incorrect format", line)
        exit
      }
     }

    //edges.cache()
    println("starting map phase2");
    val vertices: RDD[(VertexId, String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln) && isVertexLineLG(ln)).flatMap { line =>
        val fields = getFieldsFromLineLG(line)
        if(fields.length != 3) {
          println("Incorrect graph format", line)
          exit
        }
        val node1 = getVertex_FromString(fields(1))
        val node2 = getVertex_FromString("Type_" + fields(2))
        
        Array(node1, node2)
        
    }
  
    println("starting map phase3 > Building graph");
    val graph = Graph(vertices, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
  
    graph.vertices.foreach(v => println(v._2))
    graph.edges.foreach(println(_))
    return graph
  }
  
  
  def dumpGraphObj(vertexFile: String, edgeFile:String, graph: Graph[String,String]) :Unit = {
    println("saving graph as text file")
    graph.edges.saveAsObjectFile(edgeFile)
    graph.vertices.saveAsObjectFile(vertexFile)
  }
 
  def dumpGraphObjNbrs(vertexFile: String, edgeFile:String, graph: Graph[(String, Set[(Long, String, String)]),String]) :Unit = {
    println("saving graph as text file")
    graph.edges.saveAsObjectFile(edgeFile)
    graph.vertices.saveAsObjectFile(vertexFile)
  }
 
  def getGraphObj(vertexFile: String, edgeFile:String, sc : SparkContext): Graph[String,String] = {
    
    val vertices: RDD[(VertexId,String)] = sc.objectFile(vertexFile, 8)
    val edges: RDD[Edge[String]] = sc.objectFile(edgeFile, 8)
    println("starting phase > Building graph")
    val graph : Graph[String,String] = Graph(vertices, edges)
 
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
    return graph
    
  }


   def getGraphObj_KGEdge(vertexFile: String, edgeFile:String, sc : SparkContext): Graph[String,KGEdge] = {

    val vertices: RDD[(VertexId,String)] = sc.objectFile(vertexFile, 8)
    val edges: RDD[Edge[KGEdge]] = sc.objectFile(edgeFile, 8)
    println("starting phase > Building graph")
    val graph : Graph[String,KGEdge] = Graph(vertices, edges)

    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
    return graph

  }

  
  def getGraphObjNbrs(vertexFile: String, edgeFile:String, sc : SparkContext): Graph[(String, Set[(Long, String, String)]),String] = {

    val vertices: RDD[(VertexId,(String, Set[(Long, String, String)]))] = sc.objectFile(vertexFile, 16)
    val edges: RDD[Edge[String]] = sc.objectFile(edgeFile, 16)
    println("starting phase > Building graph")
    val graph : Graph[(String, Set[(Long, String, String)]),String] = Graph(vertices, edges)

    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
    return graph

  }

  def getGraphLongPredicate(filename : String, sc : SparkContext): Graph[String, Long] = {
    println("starting map phase1");
    val edges: RDD[Edge[Long]] =
      sc.textFile(filename).map { line =>
        val fields = getFieldsFromLine(line)
        Edge(fields(0).hashCode().toLong,
          fields(2).hashCode().toLong, fields(1).hashCode().toLong)
      }
    println("starting map phase2");
    val vertextRDD1: RDD[(VertexId, String)] =
      sc.textFile(filename).map { line =>
        val fields = getFieldsFromLine(line)
        (fields(0).hashCode().toLong, fields(0))
      }
    println("starting map phase3");
    val vertextRDD2: RDD[(VertexId, String)] =
      sc.textFile(filename).map { line =>
        val fields = getFieldsFromLine(line)
        (fields(2).hashCode().toLong, fields(2))
      }
    println("starting map phase4 > doing union");
    val allvertex = vertextRDD1.union(vertextRDD2)
    //println(edges.count);
    println("starting map phase5 . Building graph");
    //val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")
    val graph = Graph(allvertex, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)

    return graph
  }
 
    def getGraphLongPredicateNoVertexLabel(filename : String, sc : SparkContext): Graph[Long, Long] = {
    println("starting map phase1");
    val edges: RDD[Edge[Long]] =
      sc.textFile(filename).map { line =>
        val fields = getFieldsFromLine(line)
        Edge(fields(0).hashCode().toLong,
          fields(2).hashCode().toLong, fields(1).hashCode().toLong)
      }
    println("starting map phase2");
    val vertextRDD1: RDD[(VertexId, Long)] =
      sc.textFile(filename).map { line =>
        val fields = getFieldsFromLine(line)
        (fields(0).hashCode().toLong, fields(0).hashCode().toLong)
      }
    println("starting map phase3");
    val vertextRDD2: RDD[(VertexId, Long)] =
      sc.textFile(filename).map { line =>
        val fields = getFieldsFromLine(line)
        (fields(2).hashCode().toLong, fields(2).hashCode().toLong)
      }
    println("starting map phase4 > doing union");
    val allvertex = vertextRDD1.union(vertextRDD2)
    //println(edges.count);
    println("starting map phase5 . Building graph");
    //val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")
    val graph = Graph(allvertex, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)

    return graph
  }
  
  
  def dumpGraph(args: Array[String], sc : SparkContext, filename: String): Unit = {

    val edges: RDD[Edge[String]] =
      sc.textFile(filename).map { line =>
        val fields = line.split("\\t")
        Edge(fields(0).hashCode().toLong, fields(2).hashCode().toLong, fields(1))
      }

    val vertextRDD1: RDD[(VertexId, String)] =
      sc.textFile(filename).map { line =>
        val fields = line.split("\\t")
        (fields(0).hashCode().toLong, fields(0))
      }

    val vertextRDD2: RDD[(VertexId, String)] =
      sc.textFile(filename).map { line =>
        val fields = line.split("\\t")
        (fields(2).hashCode().toLong, fields(2))
      }
    val allvertex = vertextRDD1.union(vertextRDD2)
    println(edges.count);

    //val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")
    val graph = Graph(allvertex, edges);
    println("edge User " + graph.edges.count)
    println("Vefr" + graph.vertices.count)
    //    graph.triplets.map(
    //      triplet => triplet.srcAttr + "\t" + triplet.attr + "\t" + triplet.dstAttr).collect.foreach(writerSG.println(_))
    //

    //val v: VertexRDD[String] = graph.vertices.filter { case (id, (name)) => name.toLowerCase().contains("apple") }
    /*
    val allAppleV: VertexRDD[String] = graph.vertices.filter { case (id, (name)) => name.toLowerCase().contains("apple")  }
    
    val allAppleMeans: RDD[Edge[String]] = graph.edges.filter {e  => {
       e.attr.equals("yago:means")
    } }
    */
    /*
    val a = graph.triplets.filter(triplet => { (triplet.srcAttr.toLowerCase().contains("apple")) && (triplet.attr.contains("yago:means")) }).collect
    a.foreach(f => {
      writerSGApple.println(f.srcAttr + "\t" + f.attr + "\t" + f.dstAttr)
      val towhope = graph.triplets.filter(triplet => {
        (triplet.srcAttr.toLowerCase().contains("apple")) &&
          ((triplet.attr == "<hasWikipediaUrl>"))
      }).collect
      println("two hopes" + towhope.length)
      towhope.foreach(t => {
        writerSGApple.println(t.srcAttr + "\t" + t.attr + "\t" + t.dstAttr)
      })
    })

    writerSGApple.flush();
*/
  }
 
  
  def filter_Temporal_Graph(graph: Graph[(String, Map[String, Set[Int]]), KGEdge],
      end_time:Long) 
  :Graph[(String, Map[String, Set[Int]]), KGEdge] =
  {
    return graph.subgraph(epred => (epred.attr.getdatetime > end_time))    
  }
  
    //Called by Graph Miner main class
  def getGraphFileType(filepath:String,sc:SparkContext): Graph[String, KGEdge] =
    {
      var multi_edge_graph: Graph[String, KGEdge] = null
      if (filepath.endsWith(".obj"))
        multi_edge_graph = ReadHugeGraph.getGraphObj_KGEdge(filepath + "/vertices", filepath + "/edges", sc)
      else if (filepath.endsWith(".lg"))
        multi_edge_graph = ReadHugeGraph.getGraphLG_Temporal(filepath, sc)
      else if(filepath.endsWith(".els"))
        multi_edge_graph = ReadHugeGraph.getGraphElsevier_Temporal(filepath, sc)
      else if(filepath.endsWith(".tsv"))
        multi_edge_graph = ReadHugeGraph.getGraphTSV_Temporal(filepath, sc)  
      else
        multi_edge_graph = ReadHugeGraph.getTemporalGraph(filepath, sc)
        //multi_edge_graph = ReadHugeGraph.getGraph(filepath,sc)

        return multi_edge_graph.subgraph( vpred = (vid, attr) => attr != null )
    }
  
  def getGraphFileTypeInt(filepath:String,sc:SparkContext): Graph[Int, KGEdgeInt] =
    {
	  if (filepath.endsWith(".lg"))
        return ReadHugeGraph.getGraphLG_TemporalInt(filepath, sc)
      else 
    return ReadHugeGraph.getTemporalGraphInt(filepath, sc)
    }
  /*

 def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Load Huge Graph Main").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val urlString = "http://localhost:8983/solr/aristotle0";
    val solr = new HttpSolrServer(urlString);

    val t0 = System.currentTimeMillis();
    println("Reading graph[Int, Int] Start")
    val graph: Graph[String, String] = ReadHugeGraph.getGraph(args(0), sc)

    //val g : Graph[Int, Int] =  ReadHugeGraph.getEdgeListFile(args(0), sc)
    //val g : Graph[Long, Long] =  ReadHugeGraph.getGraphLongPredicateNoVertexLabel(args(0), sc)
    println("Reading graph[Int, Int] Done")
    // println("Indexing Starting")

    //SolrIndexBuilderFromLabel.indexGraph(graph, args(0))

    val t1 = System.currentTimeMillis();
    println("Time to load graph is(in seconds): " + (t1 - t0) / 1000);

  }
  * 
  */
 
   
}

