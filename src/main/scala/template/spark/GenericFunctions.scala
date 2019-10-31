package template.spark

import org.apache.spark.sql

object GenericFunctions {


  def SaveData(fileName:String,data:sql.DataFrame): Unit =
  {
    data.write.parquet(fileName)
  }

}
