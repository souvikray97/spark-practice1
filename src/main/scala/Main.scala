package org.souvik.application

import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder()
    .appName("spark-practice2")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

    val df: DataFrame =spark
      .read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    df.select("Date","Open","Close").show()
    val column = df.apply("Date")
    col("Date")
    import spark.implicits._
    $"Date"

    df.select(column, col("Open"),$"Close")

    val column2 = col("Open")
    val newColumn = (column2 + 2.0).as("OpenIncreasedBy2")
    val columnString = column2.cast(StringType).as("OpenAsString")

    val litcolumn = lit(2.0)
    val newColumnString = concat(columnString, lit("Hello World"))

    df.select(column, newColumn, columnString).show()

    df.select(column, newColumn, columnString)
      .filter(newColumn > 2.0)
      .filter(newColumn > column2)
      .show()

    df.select(column, newColumn, newColumnString).show(truncate = false)

  }
  }
