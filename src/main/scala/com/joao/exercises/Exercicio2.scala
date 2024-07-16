package com.joao.exercises

import com.joao.utils.SparkUtils

object Exercicio2 {
  def run(): Unit = {
    val spark = SparkUtils.initSpark()


      import spark.implicits._

      // Carregua o dataset
      val df = SparkUtils.loadAppsData(spark)


      // Remova linhas com Rating NaN; filtra as Apps com "Rating" >= 4.0 e ordena em ordem decrescente
      val highlyRatedAppsDF = df
        .filter(!$"Rating".isNaN && !$"Rating".isNull)
        .filter($"Rating" >= 4.0)
        .orderBy($"Rating".desc)

    //Salva o dataframe com um nome específico
    SparkUtils.writeCSVWithSpecificFileName(
      spark,
      highlyRatedAppsDF,
      "src/main/scala/com/joao/output2",
      "best_apps.csv",
      "§"
    )

    println("Exercício 2 concluído. Resultados salvos em src/main/scala/com/joao/output2/best_apps.csv")
  }
}