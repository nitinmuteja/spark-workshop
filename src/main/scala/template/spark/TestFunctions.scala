package template.spark

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object TestFunctions {

  def addNaturalNumbers(n: Int)(implicit spark: SparkSession): Long = {
    spark.range(1, n + 1).reduce(_ + _)
  }


  def importDataSets(implicit spark: SparkSession) = {

    val circuits = spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
    val constructorResults = spark.read.format("csv").option("header", "true").load("src/main/resources/constructorResults.csv")
    val constructor = spark.read.format("csv").option("header", "true").load("src/main/resources/constructor.csv")
    val constructorStandings = spark.read.format("csv").option("header", "true").load("src/main/resources/constructorStandings.csv")
    val drivers = spark.read.format("csv").option("header", "true").load("src/main/resources/drivers.csv")
    val driverStandings = spark.read.format("csv").option("header", "true").load("src/main/resources/driverStandings.csv")
    val lapTimes = spark.read.format("csv").option("header", "true").load("src/main/resources/lapTimes.csv")
    val pitStops = spark.read.format("csv").option("header", "true").load("src/main/resources/pitStops.csv")
    val qualifying = spark.read.format("csv").option("header", "true").load("src/main/resources/qualifying.csv")
    val races = spark.read.format("csv").option("header", "true").load("src/main/resources/races.csv")
    val results = spark.read.format("csv").option("header", "true").load("src/main/resources/results.csv")
    val seasons = spark.read.format("csv").option("header", "true").load("src/main/resources/seasons.csv")
    val status = spark.read.format("csv").option("header", "true").load("src/main/resources/status.csv")

    (circuits, constructorResults, constructor, constructorStandings, drivers, driverStandings, lapTimes, pitStops, qualifying, races, results, seasons, status)

  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }

  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }


  def importCircuitDataSet(implicit spark: SparkSession) = {
    spark.read.format("csv").option("header", "true").load("src/main/resources/circuits.csv")
  }

  def totalRacesByNationality(implicit dataSets: (sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame,
    sql.DataFrame)) = {

    dataSets

  }


}
