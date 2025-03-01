package inyeccion

/*
AUTOR: RICARDO ESPINOSA
*/

import model.caseClassMovie.Movie

import play.api.libs.json._

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}

object limpieza {
  //FUNCION PARA LIMPIAR BOOLEANOS
  def transformarBooleano(original: String): String = {
    original.trim.toLowerCase match {
      case "TRUE" | "t" | "true" => "1"
      case "FALSE" | "f" | "false" | "" => "0"
      case _ => "0"
    }
  }

  //FUNCION PARA LIMPIAR STRINGS
  def cleanString(original: String): String = {
    val cadena_original = original.trim
    if (cadena_original.isEmpty) {
      "NULL"
    }else{
      cadena_original
    }
  }

  //FUNCION PARA LIMPIAR ENTEROS
  def cleanInt(original: Int): Int = {
    original match {
      case x if x < 0 => 0
      case x          => x
    }
  }

  //FUNCION PARA LIMPIAR ENTEROS GRANDES (LONG)
  def cleanLong(original: Long): Long = {
    original match {
      case x if x < 0 => 0
      case x => x
    }
  }

  //FUNCION PARA LIMPIAR REALES
  def cleanDouble(original: Double): Double = {
    original match {
      case x if x < 0 => 0
      case x => x
    }
  }

  def limpiarComillasInternas(jsonString: String): String = {
    val regexComillasDobles = "\"([^\"]*)\"".r //Regex para encontrar fragmentos entre comillas dobles
    // Procesar cada fragmento delimitado por comillas dobles
    regexComillasDobles.replaceAllIn(jsonString, m => {
      val contenido = m.group(1) // Extraer el contenido entre comillas dobles
      // Solo limpiar si contiene comillas simples internas
      if (contenido.contains("'")) {
        val contenidoLimpio = contenido.replace("'", "") // Eliminar comillas simples internas
        s"\"$contenidoLimpio\"" // Reconstruir el fragmento limpio con comillas dobles
      } else {
        m.matched // Dejar el fragmento intacto si no tiene comillas simples internas
      }
    })
  }

  def cleanJsonLista(json: String): String = {
    try {
      //APLICAR LIMPIEZA DE COMILLAS INTERNAS
      val preProcessedJson = limpiarComillasInternas(json)

      //REALIZAR LAS TRANSFORMACIONES NECESARIAS
      val cleanedJson = preProcessedJson
        .replaceAll("'", "\"") // Cambia comillas simples por dobles
        .replaceAll("None", "null") // Cambia None por null
        .replaceAll("True", "true") // Normalizar booleano
        .replaceAll("False", "false") // Normalizar booleano
        .replaceAll("\\\\", "") // Elimina barras invertidas dobles
        .replaceAll("\\s*:\\s*", ":") // Elimina espacios alrededor de los dos puntos
        .replaceAll("\\s*,\\s*", ",") // Elimina espacios alrededor de las comas
        .replaceAll("\\s*\\{\\s*", "{") // Elimina espacios después de llaves de apertura
        .replaceAll("\\s*\\}\\s*", "}") // Elimina espacios antes de llaves de cierre
        .replaceAll("\\s*\\[\\s*", "[") // Elimina espacios después de corchetes de apertura
        .replaceAll("\\s*\\]\\s*", "]") // Elimina espacios antes de corchetes de cierre
        .replaceAll("\r?\n", "") // Elimina saltos de línea

      // Intentar parsear para validar el JSON
      val parsedJson = Json.parse(cleanedJson)
      Json.stringify(parsedJson) // Devuelve el JSON como String validado
    } catch {
      case _: Exception =>
        "[]"
    }
  }

  def cleanJsonNormal(json: String): String = {
    try {
      val preProcessedJson = limpiarComillasInternas(json)

      val cleanedJson = preProcessedJson
        .replaceAll("'", "\"") // Cambia comillas simples por dobles
        .replaceAll("None", "null") // Cambia None por null
        .replaceAll("True", "true") // Normalizar booleano
        .replaceAll("False", "false") // Normalizar booleano
        .replaceAll("\\\\", "") // Elimina barras invertidas dobles
        .replaceAll("\\s*:\\s*", ":") // Elimina espacios alrededor de los dos puntos
        .replaceAll("\\s*,\\s*", ",") // Elimina espacios alrededor de las comas
        .replaceAll("\\s*\\{\\s*", "{") // Elimina espacios después de llaves de apertura
        .replaceAll("\\s*\\}\\s*", "}") // Elimina espacios antes de llaves de cierre
        .replaceAll("\\s*\\[\\s*", "[") // Elimina espacios después de corchetes de apertura
        .replaceAll("\\s*\\]\\s*", "]") // Elimina espacios antes de corchetes de cierre
        .replaceAll("\r?\n", "") // Elimina saltos de línea

      val parsedJson = Json.parse(cleanedJson) // Intenta parsear para asegurar que es válido
      Json.stringify(parsedJson) // Devuelve el JSON como String validado
    } catch {
      case _: Exception =>
        "{}"
    }
  }

  // FUNCION PARA FORMATEO DE JSON
  def formatJsonNormal(json: String): String = {
    val cadena_original = json.trim
    if (cadena_original.isEmpty) {
      "{}"
    } else {
      if ((!cadena_original.endsWith("}"))&&(!cadena_original.startsWith("{"))) {
        "{" + cadena_original + "}"
      } else if (!cadena_original.endsWith("}")) {
        cadena_original + "}"
      } else if (!cadena_original.startsWith("{")) {
        "{" + cadena_original
      } else {
        cadena_original
      }
    }
  }

  //FUNCION PARA FORMATEO DE LISTAS DE JSON
  def formatJsonLista(json: String): String = {
    val cadena_original = json.trim
    if (cadena_original.isEmpty) {
      "[]"
    } else {
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cadena_original + "]"
      } else if (!cadena_original.endsWith("]")) {
        cadena_original + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cadena_original
      } else {
        cadena_original
      }
    }
  }

  //FUNCION PARA LIMPIAR LAS FECHAS
  def cleanDate(original: String): String = {
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    if (original.trim.isEmpty) {
      "NULL"
    } else {
      try {
        LocalDate.parse(original.trim, dateFormatter).format(dateFormatter)
      } catch {
        case _: DateTimeParseException => "NULL"
      }
    }
  }

  // LIMPIAR LOS DATOS Y ESCRIBIRLOS CON EL FORMATO DE LA NUEVA CASE CLASS MOVIE LIMPIA
  def limpiarDataSet(filasValidas: List[Movie]): List[Movie] = {
    filasValidas
      .filter(movie => movie.id != 0)  // Elimina las películas sin ID
      .distinctBy(_.id)                // Junta las películas con ID repetido
      .map { movie =>                  // Mapea las películas y las limpia

        // Transformaciones
        val adultLimpio = transformarBooleano(movie.adult)
        val videoLimpio = transformarBooleano(movie.video)

        // Limpiar cadenas
        val homepageLimpio = cleanString(movie.homepage)
        val imdbIdLimpio = cleanString(movie.imdb_id)
        val originalLanguageLimpio = cleanString(movie.original_language)
        val posterPathLimpio = cleanString(movie.poster_path)
        val statusLimpio = cleanString(movie.status)
        val taglineLimpio = cleanString(movie.tagline)
        val originalTitleLimpio = cleanString(movie.original_title)
        val overviewLimpio = cleanString(movie.overview)
        val titleLimpio = cleanString(movie.title)
        val releaseDateLimpio = cleanDate(movie.release_date)

        // Limpiar valores numéricos negativos
        val budgetLimpio = cleanInt(movie.budget)
        val runtimeLimpio = cleanInt(movie.runtime)
        val voteCountLimpio = cleanInt(movie.vote_count)
        val revenueLimpio = cleanLong(movie.revenue)
        val popularityLimpio = cleanDouble(movie.popularity)
        val voteAverageLimpio = cleanDouble(movie.vote_average)

        // Formatear JSON
        val belongsToCollectionLimpio = cleanJsonNormal(formatJsonNormal(movie.belongs_to_collection))
        val genresLimpio = cleanJsonLista(formatJsonLista(movie.genres))
        val productionCompaniesLimpio = cleanJsonLista(formatJsonLista(movie.production_companies))
        val productionCountriesLimpio = cleanJsonLista(formatJsonLista(movie.production_countries))
        val spokenLanguagesLimpio = cleanJsonLista(formatJsonLista(movie.spoken_languages))
        val keywordsLimpio = cleanJsonLista(formatJsonLista(movie.keywords))
        val castLimpio = cleanJsonLista(formatJsonLista(movie.cast))
        val crewLimpio = cleanJsonLista(formatJsonLista(movie.crew))
        val ratingsLimpio = cleanJsonLista(formatJsonLista(movie.ratings))

        // Crear nueva instancia de Movie limpia
        Movie(
          adult = adultLimpio,
          belongs_to_collection = belongsToCollectionLimpio,
          budget = budgetLimpio,
          genres = genresLimpio,
          homepage = homepageLimpio,
          id = movie.id, // Se valida al inicio con filter y distinctBy
          imdb_id = imdbIdLimpio,
          original_language = originalLanguageLimpio,
          original_title = originalTitleLimpio,
          overview = overviewLimpio,
          popularity = popularityLimpio,
          poster_path = posterPathLimpio,
          production_companies = productionCompaniesLimpio,
          production_countries = productionCountriesLimpio,
          release_date = releaseDateLimpio,
          revenue = revenueLimpio,
          runtime = runtimeLimpio,
          spoken_languages = spokenLanguagesLimpio,
          status = statusLimpio,
          tagline = taglineLimpio,
          title = titleLimpio,
          video = videoLimpio,
          vote_average = voteAverageLimpio,
          vote_count = voteCountLimpio,
          keywords = keywordsLimpio,
          cast = castLimpio,
          crew = crewLimpio,
          ratings = ratingsLimpio
        )
      }
  }
}