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
    val startReading: Double = System.nanoTime()
    val raw_df = createDataFrame()
    val endReading: Double = System.nanoTime()

    val startPreprocessing: Double = System.nanoTime()
    val processed_df = processDataFrame(raw_df)

    val endPreprocessing: Double = System.nanoTime()


    val processFileNameUDF = udf(processFileName _)


    val startPreprocessingFileName: Double = System.nanoTime()
    val df = processed_df
      .withColumn("file_name", processFileNameUDF(col("first(input_file)")))
      .drop("first(input_file)")
    val endPreprocessingFileName: Double = System.nanoTime()


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


    val startTriangles: Double = System.nanoTime()
    val trianglesDF = df.withColumn("triangles_array", countTrianglesUDF(col("edges")))
      .withColumn("triangles_count", size(col("triangles_array")))

    val endTriangles: Double = System.nanoTime()

    import spark.implicits._

    val startTopK: Double = System.nanoTime()
    val occurrence = trianglesDF.withColumn("trianglesAsStrings", explode($"triangles_array"))
      .groupBy("trianglesAsStrings")
      .count()

    occurrence.orderBy(desc("count")).limit(10).show()
    //occurrence.repartition(1).write.csv("caida_triangles.csv")


    val endTopK: Double = System.nanoTime()

    val readingDuration = (endReading - startReading) / 1e9d
    val preprocessingDuration = (endPreprocessing - startPreprocessing) / 1e9d
    val preprocessingFileNameDuration = (endPreprocessingFileName - startPreprocessingFileName) / 1e9d
    val trianglesDuration = (endTriangles - startTriangles) / 1e9d
    val topKduration = (endTopK - startTopK) / 1e9d

    println(s"Reading duration: $readingDuration sec")
    println(s"Preprocessing duration: $preprocessingDuration sec")
    println(s"Preprocessing file name duration: $preprocessingFileNameDuration sec")
    println(s"Triangle count duration: $trianglesDuration sec")
    println(s"Top k=10 duration: $topKduration sec")
  }

}