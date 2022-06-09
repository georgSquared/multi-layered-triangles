package auth.dws.mmd
package utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataHandler {
  // fetch existing global spark session
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def createDataFrame(graphs: String = "733"): DataFrame = {

    val path =
      if (graphs == "caida") "src/main/resources/as-caida"
      else "src/main/resources/as-733"

    val schema = StructType(
      Array(
        StructField("raw", StringType, nullable = true),
      )
    )

    spark.read
      .option("header", value = false)
      .option("inferSchema", value = true)
      .option("delimiter", "\n")
      .option("comment", "#")
      .schema(schema)
      .csv(path)
  }

  /*
   * Preprocess tha Dataframe so that all edge pairs are collected into an array
   * The resulting dataframe should hold a single graph in each row and an "edges" column that hold the
   * sorted array of edges
   * */
  def processDataFrame(df: DataFrame): DataFrame = {
    df.withColumn("tmp", split(col("raw"), "\\s+"))
      .withColumn("src", element_at(col("tmp"), 1).cast("int"))
      .withColumn("dest", element_at(col("tmp"), 2).cast("int"))
      .withColumn("input_file", input_file_name())
      .withColumn("graph_id", hash(col("input_file")))
      .drop("tmp", "raw", "input_file")
      .orderBy("graph_id", "src", "dest")
      .withColumn("edge_pair", sort_array(array("src", "dest")))
      .groupBy("graph_id", "edge_pair")
      .agg(first("src"), first("dest"))
      .drop("first(src)", "first(dest)")
      .groupBy("graph_id")
      .agg(collect_list("edge_pair") as "unsorted_edges")
      .withColumn("edges", sort_array(col("unsorted_edges")))
      .drop("unsorted_edges")
      .orderBy("graph_id")
  }

  /* !WIP!
   * An alternative pre-processing function better suited for creating adjacency lists
   * */
  def processDataFrameAlt(df: DataFrame): DataFrame = {
    df.withColumn("tmp", split(col("raw"), "\\s+"))
      .withColumn("src", element_at(col("tmp"), 1).cast("int"))
      .withColumn("dest", element_at(col("tmp"), 2).cast("int"))
      .withColumn("input_file", input_file_name())
      .withColumn("graph_id", hash(col("input_file")))
      .drop("tmp", "raw", "input_file")
      .groupBy("graph_id", "src")
      .agg(collect_set("dest"))
      .orderBy("graph_id", "src")
  }

  // write results into parquet files
  //    val path_to_results = "tmp/spark_output"
  //
  //    processed_df.write
  //      .format("parquet")
  //      .option("header", "true")
  //      .save(s"$path_to_results/processed.parquet")

}
