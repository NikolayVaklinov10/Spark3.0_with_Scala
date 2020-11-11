package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  

}
