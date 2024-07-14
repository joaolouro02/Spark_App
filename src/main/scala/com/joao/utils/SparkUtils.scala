// SparkUtils.scala
package com.joao.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SparkSession, DataFrame}
import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils

object SparkUtils {
  def initSpark(): SparkSession = {
    SparkSession.builder()
      .appName("Google Play Store Analysis")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
  }

  def loadUserReviews(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv("src/main/scala/com/joao/data/googleplaystore_user_reviews.csv")
  }

  def loadAppsData(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv("src/main/scala/com/joao/data/googleplaystore.csv")
  }


  def writeJSONWithSpecificFileName(spark: SparkSession, df: DataFrame, path: String, filename: String): Unit = {
    val tempPath = s"$path/temp"

    // Salva o DataFrame como um único arquivo JSON
    df.coalesce(1).write.json(tempPath)

    // Obtém o sistema de arquivos HDFS
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    // Encontra o arquivo part-* gerado
    val tempDir = new Path(tempPath)
    val partFile = fs.listStatus(tempDir)
      .map(_.getPath)
      .find(_.getName.startsWith("part-"))
      .getOrElse(throw new Exception("Arquivo part-* não encontrado"))

    // Renomeia o arquivo para o nome desejado
    val srcPath = partFile
    val destPath = new Path(s"$path/$filename")

    if (fs.exists(srcPath) && fs.isFile(srcPath)) {
      fs.rename(srcPath, destPath)

      // Remove o diretório temporário
      fs.delete(tempDir, true)
    } else {
      throw new Exception(s"Erro ao renomear o arquivo para $filename")
    }
  }

  def writeCSVWithSpecificFileName(spark: SparkSession, df: DataFrame, path: String, filename: String,delimiter: String = ","): Unit = {
    val tempPath = s"$path/temp"

    // Salva o DataFrame como um único arquivo CSV
    df.coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", "\t")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .csv(tempPath)

    // Obtém o sistema de arquivos HDFS
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    // Encontra o arquivo part-* gerado
    val tempDir = new Path(tempPath)
    val partFile = fs.listStatus(tempDir)
      .map(_.getPath)
      .find(_.getName.startsWith("part-"))
      .getOrElse(throw new Exception("Arquivo part-* não encontrado"))


    // Lê o conteúdo do arquivo
    val inputStream = fs.open(partFile)
    val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    val content = IOUtils.toString(reader)
    reader.close()
    inputStream.close()

    // Substitui as tabulações pelo delimitador desejado
    val newContent = content.replace("\t", delimiter)

    // Escreve o novo conteúdo no arquivo final
    val destPath = new Path(s"$path/$filename")
    val out = fs.create(destPath)
    IOUtils.write(newContent, out, StandardCharsets.UTF_8)
    out.close()

    // Remove o diretório temporário
    fs.delete(tempDir, true)
  }


  def writeParquetWithSpecificFileName(spark: SparkSession, df: DataFrame, path: String, filename: String): Unit = {
    val tempPath = s"$path/temp"

    // Salva o DataFrame como um único arquivo Parquet
    df.coalesce(1)
      .write
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet(tempPath)

    // Obtém o sistema de arquivos HDFS
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    // Encontra o arquivo part-* gerado
    val tempDir = new Path(tempPath)
    val partFile = fs.listStatus(tempDir)
      .map(_.getPath)
      .find(_.getName.startsWith("part-"))
      .getOrElse(throw new Exception("Arquivo part-* não encontrado"))

    // Renomeia o arquivo para o nome desejado
    val srcPath = partFile
    val destPath = new Path(s"$path/$filename")

    if (fs.exists(srcPath) && fs.isFile(srcPath)) {
      fs.rename(srcPath, destPath)

      // Remove o diretório temporário
      fs.delete(tempDir, true)
    } else {
      throw new Exception(s"Erro ao renomear o arquivo para $filename")
    }
  }

}
