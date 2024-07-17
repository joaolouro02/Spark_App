package com.joao

import com.joao.exercises._
import com.joao.utils.SparkUtils

// Inicia Spark e executa todos os exercícios
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.initSpark()

    try {
      runAllExercises()
    } finally {
      spark.stop()
    }
  }

  def runAllExercises(exercicio1: Exercicio1.type = Exercicio1,
                      exercicio2: Exercicio2.type = Exercicio2,
                      exercicio3: Exercicio3.type = Exercicio3,
                      exercicio4: Exercicio4.type = Exercicio4,
                      exercicio5: Exercicio5.type = Exercicio5): Unit = {
    executeExercise("Exercício 1", exercicio1.run())
    executeExercise("Exercício 2", exercicio2.run())
    executeExercise("Exercício 3", exercicio3.run())
    executeExercise("Exercício 4", exercicio4.run())
    executeExercise("Exercício 5", exercicio5.run())
  }

  def executeExercise(name: String, exercise: => Unit): Unit = {
    println(s"Executando $name")
    try {
      exercise
    } catch {
      case e: Exception =>
        println(s"Erro ao executar $name: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}

