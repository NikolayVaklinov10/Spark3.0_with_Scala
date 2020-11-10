package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting a.k.a. projecting
  val carNamesDF = carsDF.select(firstColumn)

  carsDF.select(
    col("Name"),
    column("Acceleration") // col and column are exactly the same
  )


}
