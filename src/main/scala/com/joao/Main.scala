package com.joao

import com.joao.exercises._
import com.joao.utils.SparkUtils

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.initSpark()

    try {
      println("Executando Exercício 1")
      Exercicio1.run()

      println("\nExecutando Exercício 2")
      Exercicio2.run()

      println("\nExecutando Exercício 3")
      Exercicio3.run()

      println("\nExecutando Exercício 4")
      Exercicio4.run()

      println("\nExecutando Exercício 5")
      Exercicio5.run()

    } finally {
      spark.stop()
    }
  }
}