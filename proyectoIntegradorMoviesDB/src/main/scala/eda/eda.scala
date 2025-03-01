// AUTOR: RICARDO E.
package eda

import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import java.io.File
import kantan.csv.ops.{toCsvInputOps, toCsvOutputOps}
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._

import play.api.libs.functional.syntax._
import play.api.libs.json._

import model.caseClassMovie.Movie
import model.caseClassJson._
import inyeccion.limpieza

object eda extends App{
  //PONER EL ARCHIVO COMPLETE PARA OBTENER LAS ESTADISTICAS REALES
  // RUTA DEL ARCHIVO DE ENTRADA
  val ruta = "src/main/resources/data/dataset/pi_movies_complete.csv"

  // LEER EL .CSV CON DELIMITADOR ';'
  val dataCSVlimpio = new File(ruta).readCsv[List, Movie](rfc.withHeader.withCellSeparator(';'))

  // FILAS VALIDAS
  val filasLeidas = dataCSVlimpio.collect { case Right(movie) => movie }
  // FILAS INVALIDAS
  val filasError = dataCSVlimpio.collect { case Left(error) => error }

  //LIMPIAR LOS DATOS
  val filasValidas = limpieza.limpiarDataSet(filasLeidas)

  println("Filas limpiadas: "+filasValidas.length)

  //DATOS BASICOS
  val maxDuration = filasValidas.map(_.runtime).maxOption.getOrElse(0)
  val averageDuration = filasValidas.map(_.runtime).sum / filasValidas.map(_.runtime).length
  val adultMovies = filasValidas.filter(_.adult == 1)


  //GENEROS MAS COMUNES EN PELICULAS
  implicit val genresFormat: OFormat[Genre] = Json.format[Genre]
  private val topGenres = filasValidas.flatMap(current => Json.parse(current.genres).as[List[Genre]])
    .map(_.name).groupBy(identity)
    .map { case (genre, occurrences) =>
      (genre, occurrences.size)
    }.toList.sortBy(-_._2)


  //PAISES QUE PRODUCEN LA MAYOR CANTIDAD DE PELICULAS
  implicit val countriesFormat: OFormat[ProductionCountry] = Json.format[ProductionCountry]
  private val topCountries = filasValidas.flatMap(current => Json.parse(current.production_countries).as[List[ProductionCountry]])
    .map(_.name).groupBy(identity)
    .map { case (country, occurrences) =>
      (country, occurrences.size)
    }.toList.sortBy(-_._2)

  //PELICULAS CON MEJOR PROMEDIO EN TMDB
  implicit val ratingsFormat : OFormat[Rating] = Json.format[Rating]
  private val topRatings = filasValidas.map{ current =>
    val totalRatings = Json.parse(current.ratings).as[List[Rating]]
    val averageRatings = ( totalRatings.map(_.rating).sum ) / totalRatings.length
    (current.title, averageRatings)
  }.sortBy(-_._2).filter(x => x._2 > 4.0)

  //PELICULAS CON MAS CALIFICACIONES EN TMDB
  private val topMoviesRatings = filasValidas.map(current =>
    (current.title, Json.parse(current.ratings).as[List[Rating]].length)).sortBy(-_._2).filter(x=> x._2 > 30000)

  //PELICULAS QUE SON PRODUCIDAS POR UN MAYOR NUMERO DE COMPAÑIAS
  implicit val productionCompaniesFormat : OFormat[ProductionCompany] = Json.format[ProductionCompany]
  private val topMoviesCompanies = filasValidas.map(current =>
      (current.title, Json.parse(current.production_companies).as[List[ProductionCompany]].length))
    .sortBy(-_._2).filter(x => x._2 >= 10)


  //PELICULAS QUE HAN CONSEGUIDO MEJOR RENTABILIDAD
  private val topProfitable = filasValidas.filter(movie => movie.budget != 0)
    .map(movie => (movie.title, math.abs(movie.revenue-movie.budget)))
    .sortBy(-_._2).filter(x => x._2 >= 250000000)

  println("------- ESTADISTICA DESCRIPTIVA ---------")
  println(s"\nPELICULA CON MAYOR DURACION: $maxDuration MINUTOS")
  println(s"\nPROMEDIO DE DURACION DE LAS PELICULAS: $averageDuration MINUTOS")
  println(s"\nPELICULAS CATALOGADAS PARA ADULTOS: ${adultMovies.length}" )


  println(s"\n|||||||||||||||||||||||||||||||||||GENEROS MAS COMUNES|||||||||||||||||||||||||||||||||||\n")
  topGenres.foreach{ case(genre, count) =>
    println(s"Genero: $genre, Ocurrencias: $count")
  }

  println(s"\n|||||||||||||||||||||||||||||||||||PAISES QUE PRODUCEN MAS PELICULAS|||||||||||||||||||||||||||||||||||\n")
  topCountries.foreach{ case(country, count) =>
    println(s"País: $country, Peliculas: $count")
  }

  println(s"\n|||||||||||||||||||||||||||||||||||PELICULAS CON MEJOR PROMEDIO DE CALIFICAIONES (MovieLens)|||||||||||||||||||||||||||||||||||\n")
  topRatings.foreach{ case(movie, rating) =>
    println(s"Película: $movie calificación promedio: ${rating}")
  }

  println(s"\n|||||||||||||||||||||||||||||||||||PELICULAS CON MAYOR NUMERO DE CALIFICACIONES (MovieLens)|||||||||||||||||||||||||||||||||||\n")
  topMoviesRatings.foreach{ case(title, ratingsNumber) =>
    println(s"Película: $title numero de ratings: $ratingsNumber.")
  }

  println(s"\n|||||||||||||||||||||||||||||||||||PELICULAS CON MAYOR NUMERO DE COMPAÑIAS DE PRODUCCION|||||||||||||||||||||||||||||||||||\n")
  topMoviesCompanies.foreach{ case(title, companiesNumber) =>
    println(s"Película: $title numero de companies: $companiesNumber.")
  }

  println(s"\n|||||||||||||||||||||||||||||||||||PELICULAS QUE MAS HAN RENTADO|||||||||||||||||||||||||||||||||||\n")
  topProfitable.foreach { case (movie, profit) =>
    println(s"Película: $movie, con ganancias totales: $profit dólares")
  }

  //Filtrar peliculas cuyo revenue sea mayor al promedio
  val promedioRevenue = (filasValidas.map(_.revenue).sum) / filasValidas.length
  val peliculasMayorRevenue = filasValidas.filter(_.revenue > promedioRevenue)
  println(s"\nNUMERO DE PELICULAS CON REVENUE MAYOR AL PROMEDIO: " + peliculasMayorRevenue.length)

  // Calcular el año más antiguo y más nuevo del que se producen las películas
  private val yearAntiguo = filasValidas.filter(!_.release_date.equalsIgnoreCase("NULL"))
    .map(current => current.release_date.split("-").head.toInt)
    .min
  private val yearNuevo = filasValidas.filter(!_.release_date.equalsIgnoreCase("NULL"))
    .map(current => current.release_date.split("-").head.toInt)
    .max

  println(s"\nAÑO DE PRODUCCION MAS ANTIGUO: $yearAntiguo. \n\nAÑO DE PRODUCCION MAS RECIENTE: $yearNuevo.")
}