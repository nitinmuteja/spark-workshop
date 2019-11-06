package template.spark


final case class Person(firstName: String, lastName: String,
                        country: String, age: Int)

object Main extends App with InitSpark {


  //  val version = spark.version
  //  println("SPARK VERSION = " + version)
  //
  //  val sumHundred = spark.range(1, 101).reduce(_ + _)
  //  println(f"Sum 1 to 100 = $sumHundred")
  //
  //  println("Reading from csv file: people-example.csv")
  //  val persons = reader.csv("people-example.csv").as[Person]
  //  persons.show(2)
  //  val averageAge = persons.agg(avg("age"))
  //    .first.get(0).asInstanceOf[Double]
  //  println(f"Average Age: $averageAge%.2f")
  //
  //
  //  val sum = TestFunctions.addNaturalNumbers(100)
  //  println(sum)


  def totalRacesByNationality(): Unit = {
    val resultDataSet = TestFunctions.totalRacesByNationality(circuitDataSet, raceDataSet)
    resultDataSet.show()
    GenericFunctions.SaveData("RacesByNationality", resultDataSet)
  }

  totalRacesByNationality()

  def winsByNationality(): Unit = {
    val resultDataSet = TestFunctions.totalWinsByNationality(driversDataSet, resultsDataSet)
    resultDataSet.show()
    GenericFunctions.SaveData("WinsByNationality", resultDataSet)
  }

  winsByNationality()


  def driversByNationality(): Unit = {
    val driverNationalityDataSet = TestFunctions.totalDriversByNationality(driversDataSet)
    driverNationalityDataSet.show()
  }

  driversByNationality()

  def ratioOfWonAndParticipated(): Unit = {
    val ratio = TestFunctions.ratioOfRacesWonAndParticipated(resultsDataSet  , driversDataSet)
    ratio.show()
  }

  ratioOfWonAndParticipated()

  close

}
