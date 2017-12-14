package edu.wsu.sumitpurohit.research.graphmining

import java.io.PrintWriter
import scala.io.Source

object CreatGraphFromLasAlamosData {

  def main(args: Array[String]): Unit = {
    
     /*
     * Convert Red Event Data
     */
    var inputfile = "/sumitData/work/PhD/PhdResearch/LasAlamosData/redteam.txt";
    var output_file_writer  = new PrintWriter("/sumitData/work/PhD/PhdResearch/LasAlamosData/redteam.ttl")
    var cnt = 0;
    for (line <- Source.fromFile(inputfile).getLines()) {
      cnt = cnt + 1
      val line_array = line.split(",")
      output_file_writer.println("red" +cnt + "\trdf:type\tcompromise_event" )
      output_file_writer.println("red" +cnt + "\thasTime\t"+line_array(0) )
      output_file_writer.println("red" +cnt + "\thasUserDomain\t"+line_array(1) )
      output_file_writer.println("red" +cnt + "\thasSourceComputer\t"+line_array(2) )
      output_file_writer.println("red" +cnt + "\thasDestinationComputer\t"+line_array(2) )
    }
    output_file_writer.flush()
    
    
    /*
     * Convert Netflow Data
     */
    inputfile = "/sumitData/work/PhD/PhdResearch/LasAlamosData/flows.txt";
    output_file_writer  = new PrintWriter("/sumitData/work/PhD/PhdResearch/LasAlamosData/flows.ttl")
    cnt = 0;
    for (line <- Source.fromFile(inputfile).getLines()) {
      cnt = cnt + 1
      val line_array = line.split(",")
      output_file_writer.println("flow" +cnt + "\trdf:type\tnetflow" )
      output_file_writer.println("flow" +cnt + "\thasTime\t"+line_array(0) )
      output_file_writer.println("flow" +cnt + "\thasDuration\t"+line_array(1) )
      output_file_writer.println("flow" +cnt + "\thasSourceComputer\t"+line_array(2) )
      output_file_writer.println("flow" +cnt + "\thasSourcePort\t"+line_array(3) )
      output_file_writer.println("flow" +cnt + "\thasDestinationComputer\t"+line_array(4) )
      output_file_writer.println("flow" +cnt + "\thasDestinationPort\t"+line_array(5) )
      output_file_writer.println("flow" +cnt + "\thasProtocol\t"+line_array(6) )
      output_file_writer.println("flow" +cnt + "\thasPacketCount\t"+line_array(7))
      output_file_writer.println("flow" +cnt + "\thasByteCount\t"+line_array(8))
    }
    output_file_writer.flush()
    
    
    
    /*
     * Convert DNS Data
     */
    inputfile = "/sumitData/work/PhD/PhdResearch/LasAlamosData/dns.txt";
    output_file_writer  = new PrintWriter("/sumitData/work/PhD/PhdResearch/LasAlamosData/dns.ttl")
    cnt = 0;
    for (line <- Source.fromFile(inputfile).getLines()) {
      cnt = cnt + 1
      val line_array = line.split(",")
      output_file_writer.println("dns" +cnt + "\trdf:type\tdns_lookup" )
      output_file_writer.println("dns" +cnt + "\thasTime\t"+line_array(0) )
      output_file_writer.println("dns" +cnt + "\thasSourceComputer\t"+line_array(1) )
      output_file_writer.println("dns" +cnt + "\thasResolvedComputer\t"+line_array(2) )
    }
    output_file_writer.flush()
    
    
    /*
     * Convert Proc Data
     */
    inputfile = "/sumitData/work/PhD/PhdResearch/LasAlamosData/proc.txt";
    output_file_writer  = new PrintWriter("/sumitData/work/PhD/PhdResearch/LasAlamosData/proc.ttl")
    cnt = 0;
    for (line <- Source.fromFile(inputfile).getLines()) {
      cnt = cnt + 1
      val line_array = line.split(",")
      output_file_writer.println("proc" +cnt + "\trdf:type\tprocess_event" )
      output_file_writer.println("proc" +cnt + "\thasTime\t"+line_array(0) )
      output_file_writer.println("proc" +cnt + "\thasUserDomain\t"+line_array(1) )
      output_file_writer.println("proc" +cnt + "\thasComputer\t"+line_array(2) )
      output_file_writer.println("proc" +cnt + "\thasProcessName\t"+line_array(3) )
      output_file_writer.println("proc" +cnt + "\thasProcessEventStatus\t"+line_array(4) )
    }
    output_file_writer.flush()
    
    
    /*
     * Convert Authentication data
     */
    inputfile = "/sumitData/work/PhD/PhdResearch/LasAlamosData/auth.txt";
    output_file_writer  = new PrintWriter("/sumitData/work/PhD/PhdResearch/LasAlamosData/auth.ttl")
    cnt = 0;
    for (line <- Source.fromFile(inputfile).getLines()) {
      cnt = cnt + 1
      val line_array = line.split(",")
      output_file_writer.println("auth" +cnt + "\trdf:type\tauthentication_attempt" )
      output_file_writer.println("auth" +cnt + "\thasTime\t"+line_array(0) )
      output_file_writer.println("auth" +cnt + "\thasSourceUserDomain\t"+line_array(1) )
      output_file_writer.println("auth" +cnt + "\thasDestinationUserDomain\t"+line_array(2) )
      output_file_writer.println("auth" +cnt + "\thasSourceComputer\t"+line_array(3) )
      output_file_writer.println("auth" +cnt + "\thasDestinationComputer\t"+line_array(4) )
      output_file_writer.println("auth" +cnt + "\thasAuthenticationType\t"+line_array(5) )
      output_file_writer.println("auth" +cnt + "\thasLogonType\t"+line_array(6) )
      output_file_writer.println("auth" +cnt + "\thasAuthenticationOrientation\t"+line_array(7) )
      output_file_writer.println("auth" +cnt + "\thasResult\t"+line_array(8) )
    }
    output_file_writer.flush()
    
  }

}