package part3typesAnddatasets

import org.apache.spark.sql.SparkSession

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()


}
