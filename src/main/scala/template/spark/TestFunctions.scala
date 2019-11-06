package template.spark

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestFunctions {

  def addNaturalNumbers(n: Int)(implicit spark: SparkSession): Long = {
    spark.range(1, n + 1).reduce(_ + _)
  }

  //  def importDataSets(implicit spark: SparkSession) = {
  //
  //    val circuits = spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  //    val constructorResults = spark.read.format("csv").option("header", "true").load("src/main/resources/constructorResults.csv")
  //    val constructor = spark.read.format("csv").option("header", "true").load("src/main/resources/constructor.csv")
  //    val constructorStandings = spark.read.format("csv").option("header", "true").load("src/main/resources/constructorStandings.csv")
  //    val drivers = spark.read.format("csv").option("header", "true").load("src/main/resources/drivers.csv")
  //    val driverStandings = spark.read.format("csv").option("header", "true").load("src/main/resources/driverStandings.csv")
  //    val lapTimes = spark.read.format("csv").option("header", "true").load("src/main/resources/lapTimes.csv")
  //    val pitStops = spark.read.format("csv").option("header", "true").load("src/main/resources/pitStops.csv")
  //    val qualifying = spark.read.format("csv").option("header", "true").load("src/main/resources/qualifying.csv")
  //    val races = spark.read.format("csv").option("header", "true").load("src/main/resources/races.csv")
  //    val results = spark.read.format("csv").option("header", "true").load("src/main/resources/results.csv")
  //    val seasons = spark.read.format("csv").option("header", "true").load("src/main/resources/seasons.csv")
  //    val status = spark.read.format("csv").option("header", "true").load("src/main/resources/status.csv")
  //
  //    (circuits, constructorResults, constructor, constructorStandings, drivers, driverStandings, lapTimes, pitStops, qualifying, races, results, seasons, status)
  //
  //  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }

  def importRacesDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/races.csv")
  }

  def importResultsDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/results.csv")
  }

  def importDriversDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/drivers.csv")
  }


  def totalRacesByNationality(raceDataSet: sql.DataFrame, circuitDataSet: sql.DataFrame) = {
    raceDataSet.join(circuitDataSet, "circuitId").groupBy("country").count().sort("count")
  }

  def totalWinsByNationality(driverDataSet: sql.DataFrame, resultsDataSet: sql.DataFrame) = {
    driverDataSet.join(resultsDataSet, "driverId").groupBy("nationality").count().sort(desc("count"))
  }


  def totalDriversByNationality(driverDataSet: sql.DataFrame) = {
    driverDataSet.groupBy("nationality").count().sort(desc("count"))
  }

  def ratioOfRacesWonAndParticipated(resultsDataSet: sql.DataFrame, driversDataSet: sql.DataFrame) = {
    val totalRaces = driversDataSet.join(resultsDataSet, "driverId").groupBy("nationality", "raceId").count().groupBy("nationality").count().select(col("nationality"),col("count").as("participationcount"))
    val racesWon = driversDataSet.join(resultsDataSet, "driverId").where("rank == 1").groupBy("nationality").count().select(col("nationality"),col("count").as("wincount"))
    totalRaces.join(racesWon, Seq("nationality"), "left_outer").select("nationality", "participationcount","wincount").na.fill(0, Seq("wincount"))
  }
}
