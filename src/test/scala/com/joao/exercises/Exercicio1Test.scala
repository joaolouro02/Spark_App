package com.joao.exercises

import com.joao.utils.SparkUtils
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import java.nio.file.{Files, Paths}

class Exercicio1Test extends FunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("Exercicio1Test")
    .master("local[*]")
    .getOrCreate()

  test("loadUserReviews carrega DataFrame corretamente") {
    val userReviewsDF = SparkUtils.loadUserReviews(spark)
    assert(!userReviewsDF.isEmpty)
    assert(userReviewsDF.columns.contains("App"))
    assert(userReviewsDF.columns.contains("Sentiment_Polarity"))
  }

  test("Sentiment_Polarity deve ser double e NaN substituído por 0") {
    val userReviewsDF = SparkUtils.loadUserReviews(spark)
    val preparedDF = userReviewsDF
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
      .na.fill(0, Seq("Sentiment_Polarity"))

    assert(preparedDF.schema("Sentiment_Polarity").dataType == DoubleType)
    assert(preparedDF.filter(col("Sentiment_Polarity").isNull).count() == 0)
  }

  test("Agregação de média de Sentiment_Polarity por App") {
    val userReviewsDF = SparkUtils.loadUserReviews(spark)
    val preparedDF = userReviewsDF
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
      .na.fill(0, Seq("Sentiment_Polarity"))

    val df_1 = preparedDF
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    assert(df_1.columns.contains("App"))
    assert(df_1.columns.contains("Average_Sentiment_Polarity"))
  }

  test("writeCSVWithSpecificFileName escreve arquivo corretamente") {
    val df: DataFrame = spark.createDataFrame(Seq(
      ("App1", 0.5),
      ("App2", 1.0),
      ("App3", -0.2),
      ("App1", 0.7),
      ("App2", 0.8),
      ("App4", 0.3)
    )).toDF("App", "Sentiment_Polarity")

    val aggregatedDF = df.groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    SparkUtils.writeCSVWithSpecificFileName(spark, aggregatedDF, "src/test/resources/output1", "df_1_test.csv")
    assert(Files.exists(Paths.get("src/test/resources/output1/df_1_test.csv")))
  }
}