package part2dataframes

import org.apache.spark.sql.SparkSession

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()
  


}
