// AUTOR: RICARDO E.
package moviesCRUD

import sqlQuerys.movieDBQuerys._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.implicits._


object programaCRUD extends App {

  private val MENU =
    """---------------
      |1. Consultar película por ID
      |2. Consultar películas de un país
      |3. Consultar películas por año de lanzamiento
      |4. Consultar películas por un mínimo de presupuesto
      |5. Consultar películas que contienen una palabra clave
      |6. Salir
      |""".stripMargin


  println("----- BIENVENIDO AL SISTEMA DE CONSULTAS DE MOVIE_DB --------")
  var selection = 0

  do {
    println(MENU)
    selection = scala.io.StdIn.readInt()

    selection match {
      case 1 => searchById()
      case 2 => searchByCountry()
      case 3 => searchByYear()
      case 4 => searchByBudget()
      case 5 => searchByKeyword()
      case _ => // Base case
    }
  } while (selection != 6)

  println("------ SALIENDO DEL SISTEMA --------")


  /*
Funciones del menú principal
 --------->  HECHO POR: CARLOSUTPL <-------------
 */


  private def searchById(): Unit = {
    print("Escriba el ID de la película a consultar: ")
    val id = scala.io.StdIn.readInt()
    val movieResult = searchMovieById(id).unsafeRunSync()

    println("------ RESULTADOS --------")
    movieResult match {
      case Some((id, title, releaseDate, language)) =>
        println(s"ID de película: $id. Título: $title, Fecha lanzamiento: $releaseDate, Lenguaje: $language")
      case None =>
        println("No movie found.")
    }
  }

  private def searchByCountry() = {
    print("Escriba el país de las películas a consultar: ")
    val country = scala.io.StdIn.readLine()
    val movieResult = searchMovieByCountry(country).unsafeRunSync()

    println("------ RESULTADOS --------")
    movieResult match {
      case Nil => println("No movie found.")
      case movies => movies.foreach { case (id, title, date, language, country) =>
        println(s"ID: $id || Título: $title || Fecha lanzamiento: $date || Lenguaje: $language || País: $country \n")
      }
    }
  }

  private def searchByYear() = {
    print("Escriba el año de las películas a consultar: ")
    val pais = scala.io.StdIn.readInt()
    val movieResult = searchMovieByYear(pais).unsafeRunSync()

    println("------ RESULTADOS --------")
    movieResult match {
      case Nil => println("No movie found.")
      case movies => movies.foreach { case (id, title, year) =>
        println(s"ID: $id || Título: $title || Año: $year \n")
      }
    }
  }

  private def searchByBudget() = {
    print("Escriba el presupuesto mínimo de las películas a consultar: ")
    val budget = scala.io.StdIn.readInt()
    val movieResult = searchMovieByBudget(budget).unsafeRunSync()

    println("------ RESULTADOS --------")
    movieResult match {
      case Nil => println("No movie found.")
      case movies => movies.foreach { case (id, title, budget) =>
        println(s"ID: $id || Título: $title || Presupuesto: $budget \n")
      }
    }
  }

  private def searchByKeyword() = {
    print("Escriba la palabra clave de las películas a consultar: ")
    val keyword = scala.io.StdIn.readLine()
    val movieResult = searchMovieByKeyword(keyword).unsafeRunSync()

    println("------ RESULTADOS --------")
    movieResult match {
      case Nil => println("No movie found.")
      case movies => movies.foreach { case (id, title, date, language, keyword) =>
        println(s"ID: $id || Título: $title || Fecha: $date || Lenguaje: $language || Keyword: $keyword \n")
      }
    }
  }
}