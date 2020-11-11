package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition,"inner")
  guitaristsBandsDF.show

  // outer joins

  // 1) left outer joins ( everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing)
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  // 2) right outer joins ( everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing)
    guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

  // 3) outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")



}
