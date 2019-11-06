package template.spark

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestFunctionsSpec extends FlatSpec with Matchers with BeforeAndAfterAll with InitSpark {

  "Finding count of drivers by nationality" should
    "return the correct count" in {
    TestFunctions.totalDriversByNationality(driversDataSet).count() shouldEqual 41
  }

  "Most number of drivers" should
  "be 162 and should be British" in {
    TestFunctions.totalDriversByNationality(driversDataSet).take(1)(0).getLong(1) shouldEqual 162
  }

  "Finding total races by nationality" should
    "return the correct count" in {
    TestFunctions.totalRacesByNationality(raceDataSet, circuitDataSet).count() shouldEqual 32
  }

  "Number of countries where races are conducted" should
    "be 65 and number of races held from Morocco should be 1" in {
    TestFunctions.totalRacesByNationality(raceDataSet, circuitDataSet).take(1)(0).getLong(1) shouldEqual 1
  }

  "Finding number of nations from where drivers are" should
  "return the correct count" in {
    TestFunctions.totalWinsByNationality(driversDataSet, resultsDataSet).count() shouldEqual 41
  }

  "Most number of drivers who won the race" should
    "be 4150 and number of unique countries from  " in {
    TestFunctions.totalWinsByNationality(driversDataSet, resultsDataSet).take(1)(0).getLong(1) shouldEqual 4150
  }

  override def afterAll(): Unit = {
    close
  }

//  FunSuite
//  test("return the correct result"){
//    Main.addNaturalNumbers(100) shouldEqual 5051
//  }

//  FunSpec
//  describe("Add natural numbers"){
//    it("return the correct result") {
//        Main.addNaturalNumbers(100) shouldEqual 5051
//    }
//  }

// WordSpec
//  "Add N numbers" should {
//  "return the correct result" in {
//    Main.addNaturalNumbers(100) shouldEqual 5051
//  }
//}

}
