package auth.dws.mmd

import utils.DataHandler.{
  createDataFrame,
  processDataFrame,
  processDataFrameAlt,
  processFileName
}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object App {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Multi Layered Triangles")
      .master("local[*]")
      .getOrCreate()

//    Read and process dataframe from the start
    val raw_df = createDataFrame()
    val processed_df = processDataFrame(raw_df)

    val processFileNameUDF = udf(processFileName _)

    val df = processed_df
      .withColumn("file_name", processFileNameUDF(col("first(input_file)")))
      .drop("first(input_file)")

    df.show()

    def countTriangles(edges: mutable.WrappedArray[mutable.WrappedArray[Int]])
      : Array[String] = {
      val states = scala.collection.mutable.Map[String, Set[String]]()

      for (edge <- edges) {
        // Cast to strings so they can be used as map keys
        val src = edge(0).toString
        val dest = edge(1).toString

        if (states.contains(src) && states.contains(dest)) {
          val commonNodes = states(src).intersect(states(dest))
          for (node <- commonNodes) {

            val triangle = Array(src, dest, node).sorted

            if (states.contains("triangles"))
              states.update("triangles",
                            states("triangles") + triangle.mkString("-"))
            else states.update("triangles", Set(triangle.mkString("-")))
          }
        } else {
          if (states.contains(dest)) states.update(dest, states(dest) + src)
          else states.update(dest, Set(src))
        }
      }

      states("triangles").toArray
    }

    val countTrianglesUDF = udf(countTriangles _)

    df.withColumn("triangles_array", countTrianglesUDF(col("edges")))
      .withColumn("triangles_count", size(col("triangles_array")))
      .show()

  }

}
