package com.joao.exercises

import com.joao.utils.SparkUtils
import org.apache.spark.sql.functions._

object Exercicio1  {

  def run(): Unit = {
    val spark = SparkUtils.initSpark()

      val userReviewsDF = SparkUtils.loadUserReviews(spark)

      // Substituir NaN por 0 em Sentiment_Polarity antes do agrupamento
      val preparedDF = userReviewsDF
        .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
        .na.fill(0, Seq("Sentiment_Polarity"))

      // Criar o DataFrame com a média de Sentiment_Polarity por App
      val df_1 = preparedDF
        .groupBy("App")
        .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

      // Salvar o DataFrame df_1 em um arquivo CSV
      SparkUtils.writeCSVWithSpecificFileName(
        spark,
        df_1,
        "src/main/scala/com/joao/output1",
        "df_1.csv"
      )

      println("Exercício 1 concluído. Resultados salvos em src/main/scala/com/joao/output1/df_1.csv")
    }
}