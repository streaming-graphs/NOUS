package gov.pnnl.aristotle.utils

import java.io.PrintWriter
import java.io.File

object Generate_TestGraph_ToMine {

  def main(args: Array[String]): Unit = {
    
    batched_file
    //combined_file
    
  }

  def batched_file()
  {
    for (file_id <- 5 to 5) {
      val base_time_factor = file_id
      var base_time = (file_id * 14) % 60
      //14 because there are 14 triples and i am avoiding overlapping facts for now.
      val writerSG = new PrintWriter(new File("GraphMineInputTime" + file_id + ".txt"))
      val i = file_id
      writerSG.println("user" + i + "\trdf:type\t" + "person" + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("user" + i + "\tworks_At\t" + "O" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("O" + i + "\trdf:type\t" + "company" + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      var other_id =0
      for(other_id <- 0 to 10)
      {
        writerSG.println("otheruser" + other_id + "\trdf:type\t" + "person" + "\t" + "2010-01-01T" + "00:00:" + s"${base_time}" + ".000");
        writerSG.println("otheruser" + other_id + "\tworks_At\t" + "O" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time}" + ".000");

      }

      
      writerSG.println("user" + i + "\tbuys\t" + "I" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("I" + i + "\trdf:type\t" + "product" + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");

      for(other_id <- 0 to 18)
      {
    	 writerSG.println("otheruser" + other_id + "\tbuys\t" + "I" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time}" + ".000");
      }

      
      writerSG.println("user" + i + "\tfriends_With\t" + "F" + (i) + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("F" + (i) + "\trdf:type\t" + "person" + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      for(other_id <- 0 to 7)
      {
    	  writerSG.println("otheruser" + other_id + "\tfriends_With\t" + "F" + (i) + "\t" + "2010-01-01T" + "00:00:" + s"${base_time}" + ".000");
      } 
      writerSG.println("OF" + (i) + "\thas_employee\t" + "F" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("OF" + i + "\trdf:type\t" + "company" + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");

      writerSG.println("O" + i + "\tmakes\t" + "m" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("O" + i + "\tmakes\t" + "n" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      //writerSG.println("m" + i + "\trdf:type\t" + "MT" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("n" + i + "\trdf:type\t" + "NT" + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("m" + i + "\tpart_of\t" + "MT" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");
      writerSG.println("MT" + i + "\trdf:type\t" + "Sup_MT" + i + "\t" + "2010-01-01T" + "00:00:" + s"${base_time += 1; base_time}" + ".000");

      writerSG.flush()

    }
  }
  
  def combined_file()
  {
    val writerSG = new PrintWriter(new File("GraphMineInputTime.txt"))
    val seed = 2;
    for(i <- 0 to seed)
    {
      writerSG.println("user"+i+"\trdf:type\t"+"person" + "\t"+"2010-01-01T0"+i+":01:00.000");
      
      writerSG.println("user"+i+"\tworks_At\t"+"O"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("O"+i+"\trdf:type\t"+"company"+ "\t"+"2010-01-01T0"+i+":01:00.000");
      
      writerSG.println("user"+i+"\tlikes\t"+"I"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("I"+i+"\trdf:type\t"+"product"+ "\t"+"2010-01-01T0"+i+":01:00.000");
      
      writerSG.println("user"+i+"\tfriends_With\t"+"F"+(i)+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("F"+(i)+"\trdf:type\t"+"person"+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("OF"+(i)+"\thas_employee\t"+"F"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("OF"+i+"\trdf:type\t"+"company"+ "\t"+"2010-01-01T0"+i+":01:00.000");

//      writerSG.println("user"+i+"\tbuys\t"+"P"+i);
//      writerSG.println("P"+i+"\trdf:type\t"+"product");
//      writerSG.println("P"+i+"\trdf:type\t"+"super_product");
      
    }
    
    for(i<- 0 to 2)
    {
      //writerSG.println("O"+i+"\tmakes\t"+"m"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("O"+i+"\tmakes\t"+"n"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      //writerSG.println("m"+i+"\trdf:type\t"+"MT"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("n"+i+"\trdf:type\t"+"NT"+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("m"+i+"\tpart_of\t"+"MT"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      //writerSG.println("MT"+i+"\trdf:type\t"+"Sup_MT"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      
      writerSG.println("n"+i+"\tis_produced_in\t"+"State"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("State"+i+"\trdf:type\t"+"State"+"\t"+"2010-01-01T0"+i+":01:00.000");
      //writerSG.println("State"+i+"\tis_partOf\t"+"Country"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("State"+i+"\tfamous_for\t"+"National_Park"+i+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("National_Park"+i+"\trdf:type\t"+"National_Park"+ "\t"+"2010-01-01T0"+i+":01:00.000");
      //writerSG.println("National_Park"+i+"\trdf:type\t"+"Location"+ "\t"+"2010-01-01T0"+i+":01:00.000");
      //writerSG.println("National_Park"+i+"\trdf:type\t"+"Org"+ "\t"+"2010-01-01T0"+i+":01:00.000");
      writerSG.println("Country"+i+"\trdf:type\t"+"Country"+ "\t"+"2010-01-01T0"+i+":01:00.000");
      
    }
    writerSG.flush()
  }

}