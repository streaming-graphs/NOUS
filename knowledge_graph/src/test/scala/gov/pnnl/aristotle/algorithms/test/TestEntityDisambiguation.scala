package gov.pnnl.aristotle.algorithms.test;
import org.scalatest._

class TestEntityDisambiguationFeature extends FeatureSpec with GivenWhenThen {
  feature("Entity Disambiguation") {
    scenario("String based match") {
      given("Mention: Apple")
      when("Query text: Apple Computers") 
      then("Return Apple Inc")
    }

    scenario("Context derived from 1-hop neighbor/feature") {
      given("Mention: Apple")
      when("""Query text: Apple sold 10M iPads this quarter. 
          iPad is present as a neighbor in the graph""")
      then("Return Apple Inc.")
    }

    scenario("Disambiguation done with implicit context") {
      given("Mention: Apple")
      when("""Query text: Apple and Google are top competitors in mobile. 
          Google is not directly connected in the graph""")
      then("Return Apple Inc.")
      given("Mention: Apple")

      given("Mention: Paris")
      when("queried with: Paris was spotted with Madonna at the Oscar")
      then("Return Paris Hilton")

      given("Mention: Paris")
      when("queried with: I plan to travel to Paris and Rome next year")
      then("Return Paris, France")
    }
  }
}
