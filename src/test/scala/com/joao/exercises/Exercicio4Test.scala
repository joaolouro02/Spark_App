package com.joao.exercises

import com.joao.utils.SparkUtils
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Paths}

class Exercicio4Test extends FunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("Exercicio4Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df1Path = "src/test/resources/output1/df_1_test.csv"
  val df3Path = "src/test/resources/output3/df_3_test.json"


  test("DataFrame df1 é carregado corretamente do CSV") {
    // Carrega o CSV existente
    val df1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(df1Path)

    assert(!df1.isEmpty)
    assert(df1.columns.contains("App"))
    assert(df1.columns.contains("Average_Sentiment_Polarity"))
  }

  test("DataFrame df3 é carregado corretamente do JSON") {
    // Carrega o JSON existente
    val df3 = spark.read.json(df3Path)

    assert(!df3.isEmpty)
    assert(df3.columns.contains("App"))
    assert(df3.columns.contains("Rating"))
  }

  test("Os DataFrames df1 e df3 são unidos corretamente com base na coluna 'App'") {
    // Carrega os DataFrames dos arquivos criados
    val df1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(df1Path)

    val df3 = spark.read
      .option("multiline", "true")
      .json(df3Path)

    val finalDF = df3.join(df1, Seq("App"), "left_outer")

    assert(finalDF.columns.contains("App"))
    assert(finalDF.columns.contains("Average_Sentiment_Polarity"))
    assert(finalDF.columns.contains("Rating"))

  }

  test("O DataFrame final é salvo corretamente como arquivo Parquet com compressão gzip") {
    // Carrega os DataFrames dos arquivos criados anteriormente
    val df1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(df1Path)

    val df3 = spark.read
      .option("multiline", "true")
      .json(df3Path)

    // Realiza a união dos DataFrames
    val finalDF = df3.join(df1, Seq("App"), "left_outer")

    // Define o caminho para salvar o arquivo Parquet
    val outputPath = "src/test/resources/output4/df_4_test.parquet"

    // Salva o DataFrame final como arquivo Parquet com compressão gzip
    SparkUtils.writeParquetWithSpecificFileName(spark, finalDF, "src/test/resources/output4", "df_4_test.parquet")

    // Verifica se o arquivo foi criado
    assert(Files.exists(Paths.get(outputPath)))

  }
}
