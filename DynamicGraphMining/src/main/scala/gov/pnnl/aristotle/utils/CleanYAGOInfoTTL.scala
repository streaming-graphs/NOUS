package gov.pnnl.aristotle.utils

import scala.io.Source
import com.esotericsoftware.kryo.io.Output
import java.io._
import util.control.Breaks._

object CleanYAGOMInfoTTL {

  def main(args: Array[String]): Unit = {

    val filename = "/sumitData/myprojects/AIM/yagoWikipediaInfo.ttl"
    val writer = new PrintWriter(new File("yagowikiinfoFullApple.ttl"))
    val FILTER_FACTOR = 20

    //aida_means.tsv can be get from http://resources.mpi-inf.mpg.de/yago-naga/aida/download/aida_means.tsv.bz2
    /*
    var c = 1;
    for (line <- Source.fromFile(filename).getLines()) {
      breakable { //breakable not required now
        if (line.startsWith("@") || line.startsWith("#") || line.isEmpty() || c % FILTER_FACTOR != 0) { ; }
        else {
          val line2 = line.substring(0, line.lastIndexOf("."));
          val linearray = line2.split("\\t");
          if (linearray.length > 2) { writer.println(line2) }
        }
        c = c + 1
      }
    }

    //combine aida means without quote
    val filename2 = "/sumitData/myprojects/AIM/aristotle-dev/knowledge_graph/aida_means_noquote.ttl"
    c = 1
    for (line <- Source.fromFile(filename2).getLines()) {
      if (c % FILTER_FACTOR == 0)
        writer.println(line)
      c = c + 1
    }
    */
    for (line <- Source.fromFile(filename).getLines()) {
      breakable { //breakable not required now
        if (line.startsWith("@") || line.startsWith("#") || line.isEmpty() ) { ; }
        else {
          val line2 = line.substring(0, line.lastIndexOf("."));
          val linearray = line2.split("\\t");
          if ((linearray.length > 2) && (line2.toLowerCase().contains("apple")) ){ writer.println(line2) }
        }
       
      }
    }
    writer.flush();

  }

}