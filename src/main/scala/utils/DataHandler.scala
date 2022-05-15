package auth.dws.mmd
package utils

import org.apache.spark.sql.functions.{
  col,
  element_at,
  hash,
  input_file_name,
  split
}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object DataHandler {
  // fetch existing global spark session
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def createDataFrame(graphs: String = "733"): DataFrame = {

    val path =
      if (graphs == "caida") "src/main/resources/as-caida"
      else "src/main/resources/as-733"

    val schema = StructType(
      Array(
        StructField("raw", StringType, true),
      )
    )

    spark.read
      .option("header", false)
      .option("inferSchema", true)
      .option("delimiter", "\n")
      .option("comment", "#")
      .schema(schema)
      .csv(path)
  }

  def processDataFrame(df: DataFrame): DataFrame = {
    df.withColumn("tmp", split(col("raw"), "\\s+"))
      .withColumn("src", element_at(col("tmp"), 1).cast("int"))
      .withColumn("dest", element_at(col("tmp"), 2).cast("int"))
      .withColumn("input_file", input_file_name())
      .withColumn("graph_id", hash(col("input_file")))
      .drop("tmp", "raw", "input_file")
      .orderBy("graph_id", "src", "dest")
  }

}
