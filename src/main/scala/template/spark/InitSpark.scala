package template.spark

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait InitSpark {
  implicit val spark: SparkSession = SparkSession.builder()
                            .appName("Spark example")
                            .master("local[*]")
                            .config("option", "some-value")
                            .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext




  //Optional
  def reader = spark.read
               .option("header",true)
               .option("inferSchema", true)
               .option("mode", "DROPMALFORMED")
  def readerWithoutHeader = spark.read
                            .option("header",true)
                            .option("inferSchema", true)
                            .option("mode", "DROPMALFORMED")
  private def init = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }



  init


  val circuitDataSet: DataFrame = TestFunctions.importCircuitDataSet
  val raceDataSet: DataFrame = TestFunctions.importRacesDataSet
  val resultsDataSet: DataFrame = TestFunctions.importResultsDataSet
  val driversDataSet: DataFrame = TestFunctions.importDriversDataSet

  def close = {
    spark.close()
  }
}
