package com.joao.exercises

import com.joao.utils.SparkUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}

class Exercicio2Test extends FunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("Exercicio2Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("loadAppsData carrega DataFrame corretamente") {
    val appsDataDF = SparkUtils.loadAppsData(spark)
    assert(!appsDataDF.isEmpty)
    assert(appsDataDF.columns.contains("App"))
    assert(appsDataDF.columns.contains("Rating"))
  }

  test("Filtragem e ordenação de aplicativos com alta classificação") {
    val df = SparkUtils.loadAppsData(spark)

    val dfWithDoubleRating = df.withColumn("Rating", col("Rating").cast("double"))

    val highlyRatedAppsDF = dfWithDoubleRating
      .filter(!$"Rating".isNaN && !$"Rating".isNull)
      .filter($"Rating" >= 4.0)
      .orderBy($"Rating".desc)

    assert(highlyRatedAppsDF.filter($"Rating" < 4.0).count() == 0)
    val ratings = highlyRatedAppsDF.select("Rating").as[Double].collect()
    assert(ratings === ratings.sorted(Ordering[Double].reverse))
  }

  test("writeCSVWithSpecificFileName escreve arquivo corretamente") {
    val df: DataFrame = spark.createDataFrame(Seq(("Teste App", 4.5))).toDF("App", "Rating")
    SparkUtils.writeCSVWithSpecificFileName(spark, df, "src/test/resources/output2", "best_apps_test.csv", "§")
    assert(Files.exists(Paths.get("src/test/resources/output2/best_apps_test.csv")))
  }
}