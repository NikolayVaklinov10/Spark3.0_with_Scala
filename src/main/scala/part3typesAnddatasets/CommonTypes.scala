package part3typesAnddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, not}

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Types and Sets")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)
  // + multiple ways of filtering

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))

  // filter on a boolean column
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")

  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  // Numbers
  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)


}
