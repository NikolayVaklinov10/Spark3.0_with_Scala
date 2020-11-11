package part3typesAnddatasets

import org.apache.spark.sql.SparkSession

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Types and Sets")
    .config("spark.master", "local")
    .getOrCreate()



}
