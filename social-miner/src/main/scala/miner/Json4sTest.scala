package incubator
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * @author ${user.name}
 */
object App {
  
  
  def main(args : Array[String]) {
    print("Reading JSON")
    val lines = scala.io.Source.fromFile("resources/testTweet.json").getLines.toList
    val line = lines(0)
    /*
    val json = parse("""
         { "name": "joe",
           "children": [
             {
               "name": "Mary",
               "age": 5
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
       """)
    val list = for {
         JObject(child) <- json
         JField("age", JInt(age))  <- child
       } yield age
    list.foreach(println)

    val data = for {
         JObject(child) <- json
         JField("name", JString(name)) <- child
         JField("age", JInt(age)) <- child
         if age > 4
       } yield (name, age)
    data.foreach(println)
    */
    println(line)
    println("parsing json ...")
    val json = parse(line)
    val data = for {
      JObject(tweet) <- json
      JField("body", JString(body)) <- tweet
    } yield (body)
    data.foreach(println) 
    data(0).split(" ").foreach(println)
    val hashtags = data(0).split(" ").filter(t => t(0) == '#')
    println("### hashtags ###")
    hashtags.foreach(println)
  }

}
