package auth.dws.mmd

import utils.DataHandler.{createDataFrame, processDataFrame, processFileName}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable

object App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Multi Layered Triangles")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    spark.time {

      // Read and process dataframe from the start
      val startReading: Double = System.nanoTime()
      val raw_df = createDataFrame(graphs = "caida")
      raw_df.show()
      val endReading: Double = System.nanoTime()

      val startPreprocessing: Double = System.nanoTime()
      val processed_df = processDataFrame(raw_df)
      processed_df.show()
      val endPreprocessing: Double = System.nanoTime()

      val processFileNameUDF = udf(processFileName _)
      val startPreprocessingFileName: Double = System.nanoTime()
      val df = processed_df
        .withColumn("file_name", processFileNameUDF(col("first(input_file)")))
        .drop("first(input_file)")
      val endPreprocessingFileName: Double = System.nanoTime()

      // UDF to count triangles on single row graphs, based on the forward algorithm
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

              val triangle = Array(src.toInt, dest.toInt, node.toInt).sorted

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

      val startTriangles: Double = System.nanoTime()
      // Count and list triangles in each graph
      val trianglesDF = df
        .withColumn("triangles_array", countTrianglesUDF(col("edges")))
        .withColumn("triangles_count", size(col("triangles_array")))
      trianglesDF.show()
      val endTriangles: Double = System.nanoTime()

      val startTopK: Double = System.nanoTime()
      // Calculate the top triangles for each graph
      val occurrenceDF = trianglesDF
        .withColumn("trianglesAsStrings", explode($"triangles_array"))
        .groupBy("trianglesAsStrings")
        .count()
      occurrenceDF.show()
      val endTopK: Double = System.nanoTime()

      occurrenceDF.orderBy(desc("count")).limit(10).show()

      // Write the results to a single CSV
      //occurrenceDF.repartition(1).write.csv("caida_triangles.csv")

      // Running time measurements
      // Note here that is no actions are called on Dataframes, these will not be accurate in regards to each operation
      val readingDuration = (endReading - startReading) / 1e9d
      val preprocessingDuration = (endPreprocessing - startPreprocessing) / 1e9d
      val preprocessingFileNameDuration = (endPreprocessingFileName - startPreprocessingFileName) / 1e9d
      val trianglesDuration = (endTriangles - startTriangles) / 1e9d
      val topKduration = (endTopK - startTopK) / 1e9d
      val totalDuration = (endTopK - startReading) / 1e9d

      println(s"Reading duration: $readingDuration sec")
      println(s"Preprocessing duration: $preprocessingDuration sec")
      println(
        s"Preprocessing file name duration: $preprocessingFileNameDuration sec")
      println(s"Triangle count duration: $trianglesDuration sec")
      println(s"Top k=10 duration: $topKduration sec")
      println(s"Total duration: $totalDuration sec")

    }
  }

}
