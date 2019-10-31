package template.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

final case class Person(firstName: String, lastName: String,
                        country: String, age: Int)

object Main extends App with InitSpark {

  import spark.implicits._

  val version = spark.version
  println("SPARK VERSION = " + version)

  val sumHundred = spark.range(1, 101).reduce(_ + _)
  println(f"Sum 1 to 100 = $sumHundred")

  println("Reading from csv file: people-example.csv")
  val persons = reader.csv("people-example.csv").as[Person]
  persons.show(2)
  val averageAge = persons.agg(avg("age"))
    .first.get(0).asInstanceOf[Double]
  println(f"Average Age: $averageAge%.2f")


  val sum = TestFunctions.addNaturalNumbers(100)
  println(sum)


  val circuitDataSet: DataFrame = TestFunctions.importCircuitDataSet
  val raceDataSet: DataFrame = TestFunctions.importRacesDataSet
  val resultDataSet = TestFunctions.totalRacesByNationality(circuitDataSet, raceDataSet)
  resultDataSet.show()
  GenericFunctions.SaveData("RacesByNationality", resultDataSet)
  val parquetFileDF = spark.read.parquet("RacesByNationality")
  parquetFileDF.show()
  close

}
