package template.spark

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestFunctionsSpec extends FlatSpec with Matchers with BeforeAndAfterAll with InitSpark {

  "Add natural numbers" should
    "return the correct result" in {
    TestFunctions.addNaturalNumbers(100) shouldEqual 5050
  }

  it should "return the correct result 2" in {
    TestFunctions.addNaturalNumbers(100) shouldEqual 5050
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
