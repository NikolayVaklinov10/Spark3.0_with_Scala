package part2dataframes

import org.apache.spark.sql.SparkSession

object DataSources extends App {

  val spark = SparkSession
    .builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()
  

}
