package com.joao

import com.joao.exercises.{Exercicio1, Exercicio2, Exercicio3, Exercicio4, Exercicio5}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

class MainIntegrationTest extends FunSuite with MockitoSugar {

  test("Main class executes all exercises correctly") {
    // Cria objetos mock para cada exercício
    val mockExercicio1 = mock[Exercicio1.type]
    val mockExercicio2 = mock[Exercicio2.type]
    val mockExercicio3 = mock[Exercicio3.type]
    val mockExercicio4 = mock[Exercicio4.type]
    val mockExercicio5 = mock[Exercicio5.type]


    doNothing().when(mockExercicio1).run()
    doNothing().when(mockExercicio2).run()
    doNothing().when(mockExercicio3).run()
    doNothing().when(mockExercicio4).run()
    doNothing().when(mockExercicio5).run()

    // Chama o método da classe Main passando os mocks como argumentos
    Main.runAllExercises(mockExercicio1, mockExercicio2, mockExercicio3, mockExercicio4, mockExercicio5)

    // Verifica se o método run foi chamado
    verify(mockExercicio1).run()
    verify(mockExercicio2).run()
    verify(mockExercicio3).run()
    verify(mockExercicio4).run()
    verify(mockExercicio5).run()
  }
}
