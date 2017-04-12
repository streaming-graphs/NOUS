package gov.pnnl.nous.utils

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object GenUtils {

 def time[R](block: => R, function_desc : String ): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + function_desc + " =" + (t1 - t0)*1e-9 + "s")
    result
  }

   def writeToFile(filename: String, data: String) ={
     Files.write(Paths.get(filename), data.getBytes(StandardCharsets.UTF_8)) }
}

