package template.spark

import org.apache.spark.sql
import org.apache.spark.sql.SaveMode

object GenericFunctions {


  def SaveData(fileName:String,data:sql.DataFrame): Unit =
  {
    data.write.mode(SaveMode.Overwrite).parquet(fileName)
  }

}
