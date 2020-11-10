package part2dataframes

import org.apache.spark.sql.SparkSession

object DataFramesBasics extends App {

  // creating a SparkSession
  val spark = SparkSession
    .builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading Data Frame
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing a Data Frame
  firstDF.show()

}
