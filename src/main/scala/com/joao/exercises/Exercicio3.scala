package com.joao.exercises

import com.joao.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Exercicio3 {

  def run(): Unit = {
    val spark = SparkUtils.initSpark()


      import spark.implicits._

      // Leitura do CSV
      val df = SparkUtils.loadAppsData(spark)

      // Preparação e limpeza dos dados
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

      SparkUtils.writeJSONWithSpecificFileName(
        spark,
        dfGrouped,
        "src/main/scala/com/joao/output3",
        "df_3.json"
      )

      println("Exercício 3 concluído. Resultados salvos em src/main/scala/com/joao/output3/df_3.json")

  }

}
