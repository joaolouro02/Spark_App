package com.joao.exercises

import com.joao.utils.SparkUtils
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import java.nio.file.{Files, Paths}

class Exercicio5Test extends FunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("Exercicio5Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("Testes integrados de Exercicio5") {
    // Carregar DataFrames existentes
    val df1 = spark.read.option("header", "true").csv("src/test/resources/output1/df_1_test.csv")
    val dfGrouped = spark.read.json("src/test/resources/output3/df_3_test.json")

    // Teste: Coluna 'Genres' é explodida corretamente no DataFrame dfExploded
    val dfExploded = dfGrouped
      .withColumn("Genre", explode($"Genres"))
      .drop("Genres")

    assert(dfExploded.count() == 5)  // Ajuste de acordo com seu arquivo real
    assert(dfExploded.columns.contains("Genre"))
    assert(dfExploded.filter($"Genre" === "Genre1").count() == 2)

    // Teste: Os DataFrames dfExploded e df1 são unidos corretamente com base na coluna 'App'
    val dfJoined = dfExploded
      .join(df1, dfExploded("App") === df1("App"), "left")
      .select(
        dfExploded("App"),
        $"Genre",
        $"Rating".cast(DoubleType),
        $"Average_Sentiment_Polarity".cast(DoubleType)
      )

    assert(dfJoined.columns.contains("App"))
    assert(dfJoined.columns.contains("Genre"))
    assert(dfJoined.columns.contains("Rating"))
    assert(dfJoined.columns.contains("Average_Sentiment_Polarity"))
    assert(dfJoined.count() == 5)

    // Teste: O DataFrame df4 é salvo corretamente como arquivo Parquet com compressão gzip
    val df4 = dfJoined
      .groupBy("Genre")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    SparkUtils.writeParquetWithSpecificFileName(
      spark,
      df4,
      "src/test/resources/output5",
      "df_4.parquet"
    )

    assert(Files.exists(Paths.get("src/test/resources/output5/df_4.parquet")))
  }
}

