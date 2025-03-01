package inyeccion

/*AUTOR:
RICARDO E.
*/

import model.caseClassMovie.Movie
import model.caseClassJson._
import model.caseClassTable._
import sqlQuerys.movieDBQuerys

import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import java.io.File

import play.api.libs.functional.syntax._
import play.api.libs.json._

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.typesafe.config.ConfigFactory
import doobie.hikari.HikariTransactor
import doobie.implicits._


//OPCIONAL
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import cats.effect.{IO, Resource}
import cats.implicits._

object inyeccion extends App {

  /*
  ANTES DE EJECUTAR
  - ESTE SCRIPT SIRVE PARA INYECTAR DIRECTO A LA DB O GENERAR LOS INSERT INTO
  - ASEGURARSE DE HABER CREADO EL ARCHIVO .conf CON LOS DATOS DE CONECCION
  - CAMBIAR EL NOMBRE DEL ARCHIVO EN VAL "ruta"
  - ASEGURARSE DE HABER COLOCADO EL ARCHIVO: "pi_movies_complete.csv" EN LA CARPETA: src/main/resources/data
  - ASEGURARSE DE HABER CREADO PREVIAMENTE LA BASE DE DATOS CON EL ARCHIVO: "creacionDB.sql"
  - DESCOMENTAR "runInsertions().unsafeRunSync()" PARA INYECTAR LOS DATOS
   */

  //CONTADORES
  var jobId = 1
  var ratingId = 1
  var count = 1

  // RUTA DEL ARCHIVO DE ENTRADA
  val ruta = "src/main/resources/data/dataset/pi_movies_complete.csv"

  // LEER EL .CSV CON DELIMITADOR ';'
  val dataCSVLimpio = new File(ruta).readCsv[List, Movie](rfc.withHeader.withCellSeparator(';'))

  // FILAS VALIDAS
  val filasLeidas = dataCSVLimpio.collect { case Right(movie) => movie }
  // FILAS INVALIDAS
  val filasError = dataCSVLimpio.collect { case Left(error) => error }

  //IMPRIMIR INFORME DE LECTURA:
  println("FILAS LEIDAS CORRECTAMENTE EN EL DATASET ORIGINAL: "+filasLeidas.length)

  // LIMPIAR EL DATA SET
  val filasListas = limpieza.limpiarDataSet(filasLeidas)

  //IMPRIMIR INFORME DE LIMPIEZA
  println("FILAS RESULTANTES DESPUES DE LA LIMPIEZA: "+filasListas.length)

  // FORMATOS JSON
  //FORMATO DE BELONG_TO_COLLECTION
  implicit val formatoBelongToCollection: Reads[BelongToCollection] = (
    (__ \ "id").read[Int].orElse(Reads.pure(0)) and
      (__ \ "name").read[String].orElse(Reads.pure("NULL")) and
      (__ \ "poster_path").read[String].orElse(Reads.pure("NULL")) and
      (__ \ "backdrop_path").read[String].orElse(Reads.pure("NULL"))
    )(BelongToCollection.apply _)

  //FORMATO DE CAST
  implicit val formatoCast: Reads[Cast] = (
    (__ \ "cast_id").read[Int] and
      (__ \ "character").read[String] and
      (__ \ "credit_id").read[String] and
      (__ \ "gender").read[Int] and
      (__ \ "id").read[Long] and
      (__ \ "name").read[String] and
      (__ \ "order").read[Int] and
      (__ \ "profile_path").read[String].orElse(Reads.pure("NULL"))
    )(Cast.apply _)

  //FORMATO DE CREW
  implicit val formatoCrew: Reads[Crew] = (
    (__ \ "credit_id").read[String] and
      (__ \ "department").read[String] and
      (__ \ "gender").read[Int] and
      (__ \ "id").read[Long] and
      (__ \ "job").read[String] and
      (__ \ "name").read[String] and
      (__ \ "profile_path").read[String].orElse(Reads.pure("NULL"))
    )(Crew.apply _)

  //FORMATO DE GENRE
  implicit val formatoGenre = Json.format[Genre]

  //FORMATO DE KEYWORD
  implicit val formatoKeyword = Json.format[Keyword]

  //FORMATO DE PRODUCTION_COMPANY
  implicit val formatoProductionCompany = Json.format[ProductionCompany]

  //FORMATO DE PRODUCTION_COUNTRY
  implicit val formatoProductionCountry = Json.format[ProductionCountry]

  //FORMATO DE RATING
  implicit val formatoRating = Json.format[Rating]

  //FORMATO DE SPOKEN_LANGUAGE
  implicit val formatoSpokenLanguage = Json.format[SpokenLanguage]

  //CREAR LA TABLA MOVIE (PRINCIPAL)
  val movieTable: List[MovieTable] = filasListas.map { current =>
    val current_belongToCollectionParseada = Json.parse(current.belongs_to_collection).validate[BelongToCollection] match {
      case JsSuccess(value, _) =>
        value.id
      case JsError(errors) =>
        println(s"Error al parsear JSON en la fila ${current.id}: $errors")
        0
    }
    MovieTable(
      id_movie = current.id,
      title = current.title,
      original_title = current.original_title,
      imdb_id = current.imdb_id,
      budget = current.budget,
      revenue = current.revenue,
      original_language = current.original_language,
      overview = current.overview,
      release_date = current.release_date,
      run_time = current.runtime,
      status = current.status,
      tagline = current.tagline,
      video = current.video.toInt,
      adult = current.adult.toInt,
      popularity = current.popularity,
      poster_path = current.poster_path,
      homepage = current.homepage,
      vote_average = current.vote_average,
      vote_count = current.vote_count,
      id_collection = current_belongToCollectionParseada
    )
  }

  //PARSEAR Y CREAR LA TABLA COLLECTION
  val belongToCollectionParseada = filasListas.flatMap { current =>
    val json = Json.parse(current.belongs_to_collection)
    json.validate[BelongToCollection] match {
      case JsSuccess(value, _) => Some(value)
      case JsError(errors) =>
        println(s"Error al parsear JSON en la fila: $errors")
        None
    }
  }
  val collectionTable: List[CollectionTable] = belongToCollectionParseada
    .filter(_.id != 0)
    .distinctBy(_.id)
    .map { belongTo =>
      CollectionTable(
        id_collection = belongTo.id,
        name = belongTo.name,
        poster_path = belongTo.poster_path,
        backdroop_path = belongTo.backdrop_path
      )
    }

  //PARSEAR Y CREAR LA TABLA RATING
  val ratingTable = filasListas.flatMap { current =>
    val ratings = Json.parse(current.ratings).as[List[Rating]]
    ratings.map { rating =>
      val newRating = RatingTable(
        rating_id = ratingId,
        userId = rating.userId,
        rating = rating.rating,
        timestamp = rating.timestamp,
        movie_id = current.id
      )
      ratingId += 1
      newRating
    }
  }

  //PARSEAR Y CREAR LA TABLA KEYWORD_MOVIE
  val keywordMovieTable = filasListas.flatMap { current =>
    val keyword = Json.parse(current.keywords).as[List[Keyword]]
    keyword.map { kw =>
      val newFila = KeywordMovieTable(
        id_keyword = kw.id,
        id_movie = current.id
      )
      newFila
    }
  }

  //PARSEAR Y CREAR LA TABLA KEYWORD
  val keywordParseada = filasListas.map{ current =>
    Json.parse(current.keywords).as[List[Keyword]]
  }
  val keywordsUnicos: List[Keyword] = keywordParseada.flatten.distinctBy(_.id)
  val keywordTable: List[KeywordTable] = keywordsUnicos.map { current =>
    KeywordTable(
      id_keyword = current.id,
      name = current.name
    )
  }

  //PARSEAR Y CREAR LA TABLA GENDER_MOVIE
  val genderMovieTable = filasListas.flatMap { current =>
    val genderPar = Json.parse(current.genres).as[List[Genre]]
    genderPar.map { gr =>
      val newFila = GenderMovieTable(
        id_gender = gr.id,
        id_movie = current.id
      )
      newFila
    }
  }

  //PARSEAR Y CREAR LA TABLA GENDER
  val genderParseada = filasListas.map{ current =>
    Json.parse(current.genres).as[List[Genre]]
  }
  val gendersUnicos: List[Genre] = genderParseada.flatten.distinctBy(_.id)
  val genderTable: List[GenderTable] = gendersUnicos.map { current =>
    GenderTable(
      id_gender = current.id,
      name = current.name
    )
  }

  //PARSEAR Y CREAR LA TABLA LANGUAGE_MOVIE
  val languageMovieTable = filasListas.flatMap { current =>
    val languagePar = Json.parse(current.spoken_languages).as[List[SpokenLanguage]]
    languagePar.map { lg =>
      val newFila = LanguageMovieTable(
        id_language = lg.iso_639_1,
        id_movie = current.id
      )
      newFila
    }
  }

  //PARSEAR Y CREAR LA TABLA LANGUAGE
  val languageParseada = filasListas.map{ current =>
    Json.parse(current.spoken_languages).as[List[SpokenLanguage]]
  }
  val languagesUnicos: List[SpokenLanguage] = languageParseada.flatten.distinctBy(_.iso_639_1)
  val languagesActualizados: List[SpokenLanguage] = languagesUnicos.map { language =>
    language.copy(name = if (language.name.trim.isEmpty) "Desconocido" else language.name)
  }
  val languageTable: List[LanguageTable] = languagesActualizados.map { current =>
    LanguageTable(
      id_language = current.iso_639_1,
      name = current.name
    )
  }

  //PARSEAR Y CREAR LA TABLA COMPANY_MOVIE
  val companyMovieTable = filasListas.flatMap { current =>
    val prodCompanyPar = Json.parse(current.production_companies).as[List[ProductionCompany]]
    prodCompanyPar.map { pc =>
      val newFila = CompanyMovieTable(
        id_company = pc.id,
        id_movie = current.id
      )
      newFila
    }
  }

  //PARSEAR Y CREAR LA TABLA COMPANY
  val prodCompanyParseada = filasListas.map{ current =>
    Json.parse(current.production_companies).as[List[ProductionCompany]]
  }
  val prodCompanyUnicos: List[ProductionCompany] = prodCompanyParseada.flatten.distinctBy(_.id)
  val companyTable: List[CompanyTable] = prodCompanyUnicos.map { current =>
    CompanyTable(
      id_company = current.id,
      name = current.name
    )
  }

  //PARSEAR Y CREAR LA TABLA COUNTRY_MOVIE
  val countryMovieTable = filasListas.flatMap { current =>
    val prodCountryPar = Json.parse(current.production_countries).as[List[ProductionCountry]]
    prodCountryPar.map { pc =>
      val newFila = CountryMovieTable(
        id_country = pc.iso_3166_1,
        id_movie = current.id
      )
      newFila
    }
  }

  //PARSEAR Y CREAR LA TABLA COUNTRY
  val prodCountryParseada = filasListas.map{ current =>
    Json.parse(current.production_countries).as[List[ProductionCountry]]
  }
  val prodCountryUnicos: List[ProductionCountry] = prodCountryParseada.flatten.distinctBy(_.iso_3166_1)
  val countryTable: List[CountryTable] = prodCountryUnicos.map { current =>
    CountryTable(
      id_country = current.iso_3166_1,
      name = current.name
    )
  }

  //PARSEAR Y CREAR LA TABLA CAST_MOVIE
  val castMovieTable = filasListas.flatMap { current =>
    val castPar = Json.parse(current.cast).as[List[Cast]]
    castPar.map { c =>
      val newFila = CastMovieTable(
        credit_id = c.credit_id,
        cast_id = c.cast_id,
        character = c.character,
        order = c.order,
        id_person = c.id,
        id_movie = current.id
      )
      newFila
    }
  }

  //PARSEAR Y CREAR LA TABLA CAST
  val castParseada = filasListas.map{ current =>
    Json.parse(current.cast).as[List[Cast]]
  }
  val castsUnicos: List[Cast] = castParseada.flatten.distinctBy(_.id)
  val castTable: List[CastTable] = castsUnicos.map { current =>
    CastTable(
      id_person = current.id,
      name = current.name,
      gender = current.gender,
      profile_path = current.profile_path
    )
  }

  //PARSEAR Y CREAR LA TABLA JOB_DEPARTMENT
  val crewParseada = filasListas.map{ current =>
    Json.parse(current.crew).as[List[Crew]]
  }
  val crewsUnicos: List[Crew] = crewParseada.flatten.distinctBy(_.job)
  val jobDepartmentTable: List[JobDepartmentTable] = crewsUnicos.map { current =>
    val new_job = JobDepartmentTable(
      id_job = jobId,
      job = current.job,
      department = current.department,
    )
    jobId+=1
    new_job
  }

  //PARSEAR Y CREAR LA TABLA CREW
  val crewsUnicos2: List[Crew] = crewParseada.flatten.distinctBy(_.id)
  val crewTable: List[CrewTable] = crewsUnicos2.map { current =>
    CrewTable(
      id_person = current.id,
      name = current.name,
      gender = current.gender,
      profile_path = current.profile_path
    )
  }

  //PARSEAR Y CREAR LA TABLA CREW_MOVIE
  val crewMovieTable = filasListas.flatMap { current =>
    val crewPar = Json.parse(current.crew).as[List[Crew]]
    crewPar.map { c =>
      val idJob = jobDepartmentTable
        .find(jobDept => jobDept.job == c.job) // COMPARA EL JOB CURRENT CON EL JOB DE LA TABLA CATALOGO (JOB_DEPARTMENT)
        .map(_.id_job)                         // SI COINCIDEN TOMA EL ID
        .getOrElse(0)                         // POR SI NO LO ENCUENTRA

      CrewMovieTable(
        id_person = c.id,
        id_movie = current.id,
        credit_id = c.credit_id,
        id_job = idJob
      )
    }
  }

  //INSERTAR LAS RESPECTIVAS TABLAS EN LA DB

  // POBLAR LA TABLA COLLECTION EN LA DB
  def insertCollection(): IO[Unit] = {
    movieDBQuerys.insertAllCollection(collectionTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA COLLECTION : ${result.size}"))
  }

  // POBLAR LA TABLA MOVIE EN LA DB
  def insertMovie(): IO[Unit] = {
    movieDBQuerys.insertAllMovie(movieTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA MOVIE: ${result.size}"))
  }

  // POBLAR LA TABLA RATING EN LA DB
  def insertRating(): IO[Unit] = {
    movieDBQuerys.insertAllRating(ratingTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA RATING: ${result.size}"))
  }

  // POBLAR LA TABLA LANGUAGE EN LA DB
  def insertLanguage(): IO[Unit] = {
    movieDBQuerys.insertAllLanguage(languageTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA LANGUAGE: ${result.size}"))
  }

  // POBLAR LA TABLA LANGUAGE_MOVIE EN LA DB
  def insertLanguageMovie(): IO[Unit] = {
    movieDBQuerys.insertAllLanguageMovie(languageMovieTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA LANGUAGE_MOVIE: ${result.size}"))
  }

  // POBLAR LA TABLA KEYWORD EN LA DB
  def insertKeyword(): IO[Unit] = {
    movieDBQuerys.insertAllKeyword(keywordTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA KEYWORD: ${result.size}"))
  }

  // POBLAR LA TABLA KEYWORD_MOVIE EN LA DB
  def insertKeywordMovie(): IO[Unit] = {
    movieDBQuerys.insertAllKeywordMovie(keywordMovieTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA KEYWORD_MOVIE: ${result.size}"))
  }

  // POBLAR LA TABLA GENDER EN LA DB
  def insertGender(): IO[Unit] = {
    movieDBQuerys.insertAllGender(genderTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA GENDER: ${result.size}"))
  }

  // POBLAR LA TABLA GENDER_MOVIE EN LA DB
  def insertGenderMovie(): IO[Unit] = {
    movieDBQuerys.insertAllGenderMovie(genderMovieTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA GENDER_MOVIE: ${result.size}"))
  }

  // POBLAR LA TABLA COUNTRY EN LA DB
  def insertCountry(): IO[Unit] = {
    movieDBQuerys.insertAllCountry(countryTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA COUNTRY: ${result.size}"))
  }

  // POBLAR LA TABLA COUNTRY_MOVIE EN LA DB
  def insertCountryMovie(): IO[Unit] = {
    movieDBQuerys.insertAllCountryMovie(countryMovieTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA COUNTRY_MOVIE: ${result.size}"))
  }

  // POBLAR LA TABLA COMPANY EN LA DB
  def insertCompany(): IO[Unit] = {
    movieDBQuerys.insertAllCompany(companyTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA COMPANY: ${result.size}"))
  }

  // POBLAR LA TABLA COMPANY_MOVIE EN LA DB
  def insertCompanyMovie(): IO[Unit] = {
    movieDBQuerys.insertAllCompanyMovie(companyMovieTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA COMPANY_MOVIE: ${result.size}"))
  }

  // POBLAR LA TABLA CAST EN LA DB
  def insertCast(): IO[Unit] = {
    movieDBQuerys.insertAllCast(castTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA CAST: ${result.size}"))
  }

  // POBLAR LA TABLA CAST_MOVIE EN LA DB
  def insertCastMovie(): IO[Unit] = {
    movieDBQuerys.insertAllCastMovie(castMovieTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA CAST_MOVIE: ${result.size}"))
  }

  // POBLAR LA TABLA CREW EN LA DB
  def insertCrew(): IO[Unit] = {
    movieDBQuerys.insertAllCrew(crewTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA CREW: ${result.size}"))
  }

  // POBLAR LA TABLA JOB_DEPARTMENT EN LA DB
  def insertJobDepartment(): IO[Unit] = {
    movieDBQuerys.insertAllJobDepartment(jobDepartmentTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA JOB_DEPARTMENT: ${result.size}"))
  }

  // POBLAR LA TABLA CREW_MOVIE EN LA DB
  def insertCrewMovie(): IO[Unit] = {
    movieDBQuerys.insertAllCrewMovie(crewMovieTable)
      .flatMap(result => IO.println(s"NÚMERO DE REGISTROS INSERTADOS EN TABLA CREW_MOVIE: ${result.size}"))
  }

  def writeCollectionTable(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCollection("src/main/resources/data/insertIntoFiles/collectionTable.sql", collectionTable)
  }

  def writeMovieTable(): IO[Unit] = {
    movieDBQuerys.writeSQLFileMovie("src/main/resources/data/insertIntoFiles/movieTable.sql", movieTable)
  }

  def writeRatingTable(): IO[Unit] = {
    movieDBQuerys.writeSQLFileRating("src/main/resources/data/insertIntoFiles/ratingTable.sql", ratingTable)
  }

  def writeLanguage(): IO[Unit] = {
    movieDBQuerys.writeSQLFileLanguage("src/main/resources/data/insertIntoFiles/languageTable.sql", languageTable)
  }

  def writeLanguageMovie(): IO[Unit] = {
    movieDBQuerys.writeSQLFileLanguageMovie("src/main/resources/data/insertIntoFiles/languageMovieTable.sql", languageMovieTable)
  }

  def writeKeyword(): IO[Unit] = {
    movieDBQuerys.writeSQLFileKeyword("src/main/resources/data/insertIntoFiles/keywordTable.sql", keywordTable)
  }

  def writeKeywordMovie(): IO[Unit] = {
    movieDBQuerys.writeSQLFileKeywordMovie("src/main/resources/data/insertIntoFiles/keywordMovieTable.sql", keywordMovieTable)
  }

  def writeGender(): IO[Unit] = {
    movieDBQuerys.writeSQLFileGender("src/main/resources/data/insertIntoFiles/genderTable.sql", genderTable)
  }

  def writeGenderMovie(): IO[Unit] = {
    movieDBQuerys.writeSQLFileGenderMovie("src/main/resources/data/insertIntoFiles/genderMovieTable.sql", genderMovieTable)
  }

  def writeCountry(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCountry("src/main/resources/data/insertIntoFiles/countryTable.sql", countryTable)
  }

  def writeCountryMovie(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCountryMovie("src/main/resources/data/insertIntoFiles/countryMovieTable.sql", countryMovieTable)
  }

  def writeCompany(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCompany("src/main/resources/data/insertIntoFiles/companyTable.sql", companyTable)
  }

  def writeCompanyMovie(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCompanyMovie("src/main/resources/data/insertIntoFiles/companyMovieTable.sql", companyMovieTable)
  }

  def writeCast(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCast("src/main/resources/data/insertIntoFiles/castTable.sql", castTable)
  }

  def writeCastMovie(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCastMovie("src/main/resources/data/insertIntoFiles/castMovieTable.sql", castMovieTable)
  }

  def writeCrew(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCrew("src/main/resources/data/insertIntoFiles/crewTable.sql", crewTable)
  }

  def writeJobDepartment(): IO[Unit] = {
    movieDBQuerys.writeSQLFileJobDepartment("src/main/resources/data/insertIntoFiles/jobDepartmentTable.sql", jobDepartmentTable)
  }

  def writeCrewMovie(): IO[Unit] = {
    movieDBQuerys.writeSQLFileCrewMovie("src/main/resources/data/insertIntoFiles/crewMovieTable.sql", crewMovieTable)
  }

  def runWriteInsertsInto(): IO[Unit] = for {
    _ <- writeCollectionTable()
    _ <- writeMovieTable()
    //_ <- writeRatingTable()
    _ <- writeLanguage()
    _ <- writeLanguageMovie()
    _ <- writeKeyword()
    _ <- writeKeywordMovie()
    _ <- writeGender()
    _ <- writeGenderMovie()
    _ <- writeCountry()
    _ <- writeCountryMovie()
    _ <- writeCompany()
    _ <- writeCompanyMovie()
    _ <- writeCast()
    _ <- writeCastMovie()
    _ <- writeCrew()
    _ <- writeJobDepartment()
    _ <- writeCrewMovie()
  } yield ()

  def runInsertions(): IO[Unit] = for {
    _ <- insertCollection()
    _ <- insertMovie()
    _ <- insertRating()
    _ <- insertLanguage()
    _ <- insertLanguageMovie()
    _ <- insertKeyword()
    _ <- insertKeywordMovie()
    _ <- insertGender()
    _ <- insertGenderMovie()
    _ <- insertCountry()
    _ <- insertCountryMovie()
    _ <- insertCompany()
    _ <- insertCompanyMovie()
    _ <- insertCast()
    _ <- insertCastMovie()
    _ <- insertCrew()
    _ <- insertJobDepartment()
    _ <- insertCrewMovie()
  } yield ()

  //DESCOMENTAR PARA HACER LA INSERCION DIRECTA
  //runInsertions().unsafeRunSync()

  //DESCOMENTAR PARA GENERAR LOS ARCHIVOS INSERT INTO
  //runWriteInsertsInto().unsafeRunSync()

  //IMPRIMIR INFORME
  println("LOS ARCHIVOS INSERT INTO SE HAN GENERADO CORRECTAMENTE EN LA CARPETA \"data/insertIntoFiles\"")
  println("INFORME DE INYECCION DE DATOS:")
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA MOVIE : " + movieTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA RATING : " + ratingTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA KEYWORD_MOVIE : " + keywordMovieTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA KEYWORDS : " + keywordTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA GENDER_MOVIE : " + genderMovieTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA GENDER : " + genderTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA LANGUAGE_MOVIE : " + languageMovieTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA LANGUAGE : " + languageTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA COMPANY_MOVIE : " + companyMovieTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA COMPANY : " + companyTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA COUNTRY_MOVIE : " + countryMovieTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA COUNTRY : " + countryTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA COLLECTION : " + collectionTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA CAST_MOVIE : " + castMovieTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA CAST : " + castTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA JOB_DEPARTMENT : " + jobDepartmentTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA CREW : " + crewTable.length)
  println("NUMERO DE REGISTROS INYECTADOS EN TABLA CREW_MOVIE : " + crewMovieTable.length)
}