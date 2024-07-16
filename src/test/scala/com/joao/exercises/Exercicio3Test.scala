package com.joao.exercises

import com.joao.utils.SparkUtils
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.nio.file.{Files, Paths}

class Exercicio3Test extends FunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("Exercicio3Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("loadAppsData carrega DataFrame corretamente") {
    val appsDataDF = SparkUtils.loadAppsData(spark)
    assert(!appsDataDF.isEmpty)
    assert(appsDataDF.columns.contains("App"))
    assert(appsDataDF.columns.contains("Rating"))
  }

  test("DataFrame é limpo e transformado corretamente") {
    val df = SparkUtils.loadAppsData(spark)

    // Realize as mesmas transformações do Exercicio3
    val dfCleaned = df
      .withColumn("Rating", $"Rating".cast(DoubleType))
      .withColumn("Reviews", $"Reviews".cast(LongType))
      .withColumn("Size", when($"Size".endsWith("M"), regexp_replace($"Size", "M", "").cast(DoubleType)).otherwise(lit(null)))
      .withColumn("Price", regexp_replace($"Price", "\\$", "").cast(DoubleType) * 0.9)
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumn("Genres", split($"Genres", ";"))
      .withColumn("Last_Updated", to_date($"Last Updated", "MMM dd, yyyy"))
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    assert(dfCleaned.schema.fields.map(_.name).contains("Rating"))
    assert(dfCleaned.schema.fields.map(_.name).contains("Reviews"))
    assert(dfCleaned.schema.fields.map(_.name).contains("Size"))
    assert(dfCleaned.schema.fields.map(_.name).contains("Price"))
    assert(dfCleaned.schema.fields.map(_.name).contains("Content_Rating"))
    assert(dfCleaned.schema.fields.map(_.name).contains("Genres"))
    assert(dfCleaned.schema.fields.map(_.name).contains("Last_Updated"))
    assert(dfCleaned.schema.fields.map(_.name).contains("Current_Version"))
    assert(dfCleaned.schema.fields.map(_.name).contains("Minimum_Android_Version"))
  }

  test("DataFrame é agrupado e duplicatas tratadas corretamente") {
    val df = SparkUtils.loadAppsData(spark)

    // Realize as mesmas transformações do Exercicio3
    val dfCleaned = df
      .withColumn("Rating", $"Rating".cast(DoubleType))
      .withColumn("Reviews", $"Reviews".cast(LongType))
      .withColumn("Size", when($"Size".endsWith("M"), regexp_replace($"Size", "M", "").cast(DoubleType)).otherwise(lit(null)))
      .withColumn("Price", regexp_replace($"Price", "\\$", "").cast(DoubleType) * 0.9)
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumn("Genres", split($"Genres", ";"))
      .withColumn("Last_Updated", to_date($"Last Updated", "MMM dd, yyyy"))
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    // Agrupamento por App para tratar duplicatas
    val dfGrouped = dfCleaned
      .groupBy($"App")
      .agg(
        collect_set($"Category").alias("Categories"),
        max(struct($"Reviews", $"Rating", $"Size", $"Installs", $"Type", $"Price", $"Content_Rating", $"Genres", $"Last_Updated", $"Current_Version", $"Minimum_Android_Version")).alias("max_struct")
      )
      .select(
        $"App",
        $"Categories",
        $"max_struct.Rating",
        $"max_struct.Reviews",
        $"max_struct.Size",
        $"max_struct.Installs",
        $"max_struct.Type",
        $"max_struct.Price",
        $"max_struct.Content_Rating",
        $"max_struct.Genres",
        $"max_struct.Last_Updated",
        $"max_struct.Current_Version",
        $"max_struct.Minimum_Android_Version"
      )

    assert(dfGrouped.columns.contains("App"))
    assert(dfGrouped.columns.contains("Categories"))
    assert(dfGrouped.columns.contains("Rating"))
    assert(dfGrouped.columns.contains("Reviews"))
    assert(dfGrouped.columns.contains("Size"))
    assert(dfGrouped.columns.contains("Installs"))
    assert(dfGrouped.columns.contains("Type"))
    assert(dfGrouped.columns.contains("Price"))
    assert(dfGrouped.columns.contains("Content_Rating"))
    assert(dfGrouped.columns.contains("Genres"))
    assert(dfGrouped.columns.contains("Last_Updated"))
    assert(dfGrouped.columns.contains("Current_Version"))
    assert(dfGrouped.columns.contains("Minimum_Android_Version"))
  }

  test("writeJSONWithSpecificFileName escreve arquivo corretamente") {
    val df: DataFrame = spark.createDataFrame(Seq(
      ("App1", Seq("Category1"), 4.5, 100L, 15.0, "1,000+", "Free", 0.0, "Everyone", Seq("Genre1"), java.sql.Date.valueOf("2023-07-15"), "1.0.0", "4.0 and up"),
      ("App2", Seq("Category2"), 3.8, 50L, 20.0, "500+", "Paid", 2.99, "Teen", Seq("Genre2"), java.sql.Date.valueOf("2023-07-14"), "2.0.0", "5.0 and up"),
      ("App3", Seq("Category1", "Category3"), 4.2, 75L, 18.0, "5,000+", "Free", 0.0, "Everyone", Seq("Genre1", "Genre3"), java.sql.Date.valueOf("2023-07-13"), "1.5.0", "4.4 and up"),
      ("App4", Seq("Category4"), 4.0, 200L, 25.0, "10,000+", "Free", 0.0, "Mature", Seq("Genre4"), java.sql.Date.valueOf("2023-07-12"), "3.0.0", "6.0 and up")
    )).toDF("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")

    SparkUtils.writeJSONWithSpecificFileName(spark, df, "src/test/resources/output3", "df_3_test.json")
    assert(Files.exists(Paths.get("src/test/resources/output3/df_3_test.json")))
  }
}
