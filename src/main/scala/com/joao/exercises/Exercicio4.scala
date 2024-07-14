package com.joao.exercises

import com.joao.utils.SparkUtils

object Exercicio4 {

  def run(): Unit = {
    val spark = SparkUtils.initSpark()

    // Carregar df1 do CSV
    val df1 = spark.read
      .option("header", "true")
      .csv("src/main/scala/com/joao/output1/df_1.csv")

    // Carregar df3 do JSON
    val df3 = spark.read
      .json("src/main/scala/com/joao/output3/df_3.json")

    // Unir df1 e df3 com base na coluna "App"
    val finalDF = df3.join(df1, Seq("App"), "left_outer")

    // Salvar o DataFrame final como arquivo Parquet com compressão gzip
    finalDF.write
      .option("compression", "gzip")
      .parquet("src/main/scala/com/joao/output4")

    println("Exercício 4 concluído. Resultados salvos em src/main/scala/com/joao/output4")
  }
}
