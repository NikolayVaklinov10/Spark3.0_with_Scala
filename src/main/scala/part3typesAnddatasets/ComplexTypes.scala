package part3typesAnddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, datediff, to_date}

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
    .show()

}
