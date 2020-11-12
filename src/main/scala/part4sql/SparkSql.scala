package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regula DF API
  carsDF.select((col("Name"))).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |
      |""".stripMargin)

  americanCarsDF.show()

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

//  databasesDF.show()

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  employeesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")


}
