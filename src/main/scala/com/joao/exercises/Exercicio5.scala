package com.joao.exercises

import com.joao.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object Exercicio5 {

  def run(): Unit = {
    val spark = SparkUtils.initSpark()

    import spark.implicits._

    // Carregar datasets criados pelos exercícios 1 e 3
    val df1 = spark.read
      .option("header", "true")
      .csv("src/main/scala/com/joao/output1/df_1.csv")

    val dfGrouped = spark.read
      .json("src/main/scala/com/joao/output3/df_3.json")

    // Explodir a coluna de gêneros
    val dfExploded = dfGrouped
      .withColumn("Genre", explode($"Genres"))
      .drop("Genres")

    // Juntar os datasets com base no nome do aplicativo
    val dfJoined = dfExploded
      .join(df1, dfExploded("App") === df1("App"), "left")
      .select(
        dfExploded("App"),
        $"Genre",
        $"Rating".cast(DoubleType),
        $"Average_Sentiment_Polarity".cast(DoubleType)
      )

    val dfClean = dfJoined
      .filter(!$"Rating".isNull && !$"Rating".isNaN && !$"Average_Sentiment_Polarity".isNull && !$"Average_Sentiment_Polarity".isNaN)


    // Agrupar por gênero e calcular o número de aplicativos, a média de rating e a média de sentiment polarity
    val df4 = dfClean
      .groupBy("Genre")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    // Salvar o DataFrame como um único arquivo Parquet chamado df_4.parquet
    SparkUtils.writeParquetWithSpecificFileName(
      spark,
      df4,
      "src/main/scala/com/joao/output5",
      "df_4.parquet"
    )

    println("Exercício 5 concluído. Resultados salvos em src/main/scala/com/joao/output5/df_4.parquet")
  }
}

