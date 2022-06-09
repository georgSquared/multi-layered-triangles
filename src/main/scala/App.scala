package auth.dws.mmd

import utils.DataHandler.{
  createDataFrame,
  processDataFrame,
  processDataFrameAlt
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
//    val raw_df = createDataFrame()
//    val processed_df = processDataFrame(raw_df)

    val processed_df = spark.read.parquet("tmp/spark_output/processed.parquet")

    val df = processed_df.sample(0.1)
    df.show(1, false)

    val countTriangles =
      (edges: mutable.WrappedArray[mutable.WrappedArray[Int]]) => {
        val states = scala.collection.mutable.Map[String, Set[String]]()

        for (edge <- edges) {
          // Cast to strings so they can be used as map keys
          val src = edge(0).toString
          val dest = edge(1).toString
          println(s"EDGE: $edge. SRC: $src. DST: $dest")

          if (states.contains(src) && states.contains(dest)) {
            val commonNodes = states(src).intersect(states(dest))
            for (node <- commonNodes if node != src && node != dest) {
//              println("#####################")
//              println(s"Got a triangle with $src, $dest, $node")
//              println("#####################")
              val triangle = Array(src, dest, node.toString).sorted
              if (states.contains("triangles"))
                states.update("triangles",
                              states("triangles") + triangle.mkString(","))
              else states.update("triangles", Set(triangle.mkString(",")))
            }
          } else {
            if (states.contains(dest)) states.update(dest, states(dest) + src)
            else states.update(dest, Set(src))
          }
        }
        edges
          .map(x => x.length)
      }

    val countTrianglesUDF = udf(countTriangles)

    df.withColumn("temp", countTrianglesUDF(col("edges"))).show()

  }

}
