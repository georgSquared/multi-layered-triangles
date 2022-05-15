package auth.dws.mmd

import utils.DataHandler.{createDataFrame, processDataFrame}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object App {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Multi Layered Triangles")
      .master("local[*]")
      .getOrCreate()

    val df = createDataFrame()

    val processed_df = processDataFrame(df)

    processed_df.show(10000, truncate = false)

  }

}
