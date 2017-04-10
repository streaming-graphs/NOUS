package gov.pnnl.aristotle.algorithms.test;
import org.scalatest.FeatureSpec
import org.scalatest.GivenWhenThen

class TestGraphSearch extends FeatureSpec with GivenWhenThen {
  feature("Text Search") {

    scenario("Star query") {
      given("Knowledge Graph: YAGO") 
      when("Query: Which company sold 10M iPads?")
      then("return: Apple Inc")
    }
  
    scenario("Reachability query") {
      given("Knowledge Graph: YAGO") 
      when("Query: How are Apple and Corning related?")
      then("return: Apple uses Gorilla Glass, Corning produces Gorilla Glass")
    }

    scenario("Imply relationship and execute star query") {
      given("Knowledge Graph: YAGO + PIERS") 
      when("Query: Which company imports most electronics to US?")
      then("Return Apple Inc")

      given("Knowledge Graph: YAGO + PIERS") 
      when("Query: Which company exports most cars from US?")
      then("Return Ford")
    }

    scenario("Similiarity Search") {
      given("Knowledge Graph: YAGO + PIERS")
      when("Query: Which companies are similar to Ford?")
      then("Return Toyota, General Motors and Honda") 
    }

    scenario("Complex similarity search") {
      given("Knowledge Graph: YAGO + PIERS")
      when("Query: Which companies are similar to Ford and exports to Cuba?")
      then("Return Honda") 
    }

    scenario("True/False questions") {
      given("Knowledge Graph: YAGO + PIERS")
      when("Query: Does Ford import Paladium?")
      then("Return Yes, it imported 1 ton Paladium between 2010-2014") 
    }
  }
}
