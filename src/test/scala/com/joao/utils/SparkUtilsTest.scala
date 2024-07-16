package com.joao.utils

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.{SparkSession, DataFrame}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.io.File

class SparkUtilsTest extends FunSuite with BeforeAndAfter {

  var spark: SparkSession = _
  var tempDir: String = _

  // Método executado antes de cada teste
  before {
    spark = SparkSession.builder()
      .appName("TesteSparkUtils")
      .master("local")
      .getOrCreate()

    tempDir = Files.createTempDirectory("sparkTest").toString
  }

  // Método executado após cada teste
  after {
    spark.stop()
    val tempDirPath = Paths.get(tempDir) //apaga o diretório temporário
    deleteRecursively(tempDirPath.toFile)
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete()) {
      throw new Exception(s"Failed to delete ${file.getAbsolutePath}")
    }
  }



  test("initSpark inicializa SparkSession corretamente") {
    assert(spark != null)
    assert(spark.sparkContext.appName == "TesteSparkUtils")
  }


  // Verifica se o método loadUserReviews carrega corretamente o dataframe das reviews
  test("loadUserReviews carrega DataFrame corretamente") {
    val userReviewsDF = SparkUtils.loadUserReviews(spark)
    assert(!userReviewsDF.isEmpty)
    assert(userReviewsDF.columns.contains("App"))
    assert(userReviewsDF.columns.contains("Sentiment_Polarity"))
  }

  // Verifica se o método loadUserReviews carrega corretamente o dataframe das apps
  test("loadAppsData carrega DataFrame corretamente") {
    val appsDataDF = SparkUtils.loadAppsData(spark)
    assert(!appsDataDF.isEmpty)
    assert(appsDataDF.columns.contains("App"))
    assert(appsDataDF.columns.contains("Category"))
  }


  /////////// Testes para a escrita dos arquivos ///////////////////

  test("writeJSONWithSpecificFileName escreve arquivo corretamente") {
    val df: DataFrame = spark.createDataFrame(Seq(("Teste", 1))).toDF("Coluna1", "Coluna2")
    SparkUtils.writeJSONWithSpecificFileName(spark, df, tempDir, "test.json")
    assert(Files.exists(Paths.get(s"$tempDir/test.json")))
  }

  test("writeCSVWithSpecificFileName escreve arquivo corretamente") {
    val df: DataFrame = spark.createDataFrame(Seq(("Teste", 1))).toDF("Coluna1", "Coluna2")
    SparkUtils.writeCSVWithSpecificFileName(spark, df, tempDir, "test.csv")
    assert(Files.exists(Paths.get(s"$tempDir/test.csv")))
  }

  test("writeParquetWithSpecificFileName escreve arquivo corretamente") {
    val df: DataFrame = spark.createDataFrame(Seq(("Teste", 1))).toDF("Coluna1", "Coluna2")
    SparkUtils.writeParquetWithSpecificFileName(spark, df, tempDir, "test.parquet")
    assert(Files.exists(Paths.get(s"$tempDir/test.parquet")))
  }
}
