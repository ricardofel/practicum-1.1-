// RICARDO E.
package sqlQuerys

import doobie._
import doobie.implicits._

import cats.effect.unsafe.implicits.global
import cats.effect._
import cats.implicits._

import model.caseClassTable._
import sqlConfig.movieDBConfig

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import cats.effect.{IO, Resource}
import cats.implicits._

object movieDBQuerys {
  //FUNCIONES PARA CREAR LOS ARCHIVOS INSERT INTO DE CADA TABLA

  //ESCRIBIR ARCHIVO TABLA COLLECTION
  def generateInsertCollection(bel: CollectionTable): String =
    s"""INSERT INTO collection (id_collection, name, poster_path, backdrop_path)
     VALUES (
       ${bel.id_collection}, \"${bel.name}\", \"${bel.poster_path}\", \"${bel.backdroop_path}\");"""
  def writeSQLFileCollection(filePath: String, inserts: List[CollectionTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCollection).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA MOVIE
  def generateInsertMovie(mov: MovieTable): String =
    s"""INSERT INTO movie (id_movie, title, original_title, imdb_id, budget, revenue, original_language,
                        overview, release_date, run_time, status, tagline, video, adult, popularity,
                        poster_path, homepage, vote_average, vote_count, id_collection)
     VALUES (
       ${mov.id_movie},
       \"${mov.title}\",
       \"${mov.original_title}\",
       \"${mov.imdb_id}\",
       ${mov.budget},
       ${mov.revenue},
       CASE WHEN LENGTH(\"${mov.original_language}\") > 2 THEN NULL ELSE \"${mov.original_language}\" END,
       \"${mov.overview}\",
       \"${mov.release_date}\",
       ${mov.run_time},
       \"${mov.status}\",
       \"${mov.tagline}\",
       ${mov.video},
       ${mov.adult},
       ${mov.popularity},
       \"${mov.poster_path}\",
       \"${mov.homepage}\",
       ${mov.vote_average},
       ${mov.vote_count},
       CASE WHEN ${mov.id_collection} = 0 THEN NULL ELSE ${mov.id_collection} END);"""
  def writeSQLFileMovie(filePath: String, inserts: List[MovieTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertMovie).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA RATING
  def generateInsertRating(rat: RatingTable): String =
    s"""INSERT INTO rating (rating_id, userId, rating, timestamp, id_movie)
     VALUES (${rat.rating_id}, ${rat.userId},${rat.rating},${rat.timestamp},${rat.movie_id});"""
  //PARA ESTA TABLA SE ESCRIBE CADA INSERT INTO A LA VEZ PARA NO ABUSAR DE LOS RECURSOS DE MEMORIA
  def writeSQLFileRating(filePath: String, inserts: List[RatingTable]): IO[Unit] = {
    IO {
      val writer = new BufferedWriter(new FileWriter(filePath))
      try {
        inserts.foreach { rat =>
          writer.write(generateInsertRating(rat))
          writer.newLine() // Agregar salto de línea después de cada INSERT
        }
      } finally {
        writer.close()
      }
    }
  }

  //ESCRIBIR ARCHIVO TABLA LANGUAGE
  def generateInsertLanguage(lan: LanguageTable): String =
    s"INSERT INTO language (id_language, name)VALUES (\"${lan.id_language}\", \"${lan.name}\");"
  def writeSQLFileLanguage(filePath: String, inserts: List[LanguageTable]): IO[Unit] = {
    IO {
      val writer = new BufferedWriter(new FileWriter(filePath))
      try {
        inserts.foreach { rat =>
          writer.write(generateInsertLanguage(rat))
          writer.newLine() // Agregar salto de línea después de cada INSERT
        }
      } finally {
        writer.close()
      }
    }
  }

  //ESCRIBIR ARCHIVO TABLA LANGUAGEMOVIE
  def generateInsertLanguageMovie(lan: LanguageMovieTable): String =
    s"INSERT INTO language_movie (id_language, id_movie)VALUES (\"${lan.id_language}\", ${lan.id_movie});"
  def writeSQLFileLanguageMovie(filePath: String, inserts: List[LanguageMovieTable]): IO[Unit] = {
    IO {
      val writer = new BufferedWriter(new FileWriter(filePath))
      try {
        inserts.foreach { rat =>
          writer.write(generateInsertLanguageMovie(rat))
          writer.newLine() // Agregar salto de línea después de cada INSERT
        }
      } finally {
        writer.close()
      }
    }
  }

  //ESCRIBIR ARCHIVO TABLA KEYWORD
  def generateInsertKeyword(key: KeywordTable): String =
    s"INSERT INTO keyword (id_keyword, name)VALUES (${key.id_keyword}, \"${key.name}\");"
  def writeSQLFileKeyword(filePath: String, inserts: List[KeywordTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertKeyword).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA KEYWORDMOVIE
  def generateInsertKeywordMovie(key: KeywordMovieTable): String =
    s"INSERT INTO keyword_movie (id_keyword, id_movie)VALUES (${key.id_keyword}, ${key.id_movie});"
  def writeSQLFileKeywordMovie(filePath: String, inserts: List[KeywordMovieTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertKeywordMovie).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA GENDER
  def generateInsertGender(gen: GenderTable): String =
    s"INSERT INTO gender (id_gender, name)VALUES (${gen.id_gender}, \"${gen.name}\");"
  def writeSQLFileGender(filePath: String, inserts: List[GenderTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertGender).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA GENDER_MOVIE
  def generateInsertGenderMovie(gen: GenderMovieTable): String =
    s"INSERT INTO gender_movie (id_gender, id_movie) VALUES (${gen.id_gender}, ${gen.id_movie});"
  def writeSQLFileGenderMovie(filePath: String, inserts: List[GenderMovieTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertGenderMovie).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA COUNTRY
  def generateInsertCountry(coun: CountryTable): String =
    s"INSERT INTO country (id_country, name)VALUES (${coun.id_country}, \"${coun.name}\");"
  def writeSQLFileCountry(filePath: String, inserts: List[CountryTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCountry).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA COUNTRYMOVIE
  def generateInsertCountryMovie(coun: CountryMovieTable): String =
    s"INSERT INTO country_movie (id_country, id_movie)VALUES (${coun.id_country}, ${coun.id_movie});"
  def writeSQLFileCountryMovie(filePath: String, inserts: List[CountryMovieTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCountryMovie).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA COMPANY
  def generateInsertCompany(com: CompanyTable): String =
    s"INSERT INTO company (id_company, name)VALUES (${com.id_company}, \"${com.name}\");"
  def writeSQLFileCompany(filePath: String, inserts: List[CompanyTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCompany).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA COMPANYMOVIE
  def generateInsertCompanyMovie(com: CompanyMovieTable): String =
    s"INSERT INTO company_movie (id_company, id_movie)VALUES (${com.id_company}, ${com.id_movie});"
  def writeSQLFileCompanyMovie(filePath: String, inserts: List[CompanyMovieTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCompanyMovie).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA CAST
  def generateInsertCast(cast: CastTable): String =
    s"INSERT INTO cast_miembros (id_person, name, gender, profile_path)VALUES (${cast.id_person}, \"${cast.name}\", ${cast.gender},CASE WHEN \"${cast.profile_path}\" = 'NULL' THEN NULL ELSE \"${cast.profile_path}\" END);"
  def writeSQLFileCast(filePath: String, inserts: List[CastTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCast).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA CASTMOVIE
  def generateInsertCastMovie(cast: CastMovieTable): String =
    s"INSERT INTO cast_movie (credit_id, cast_id, character_movie, order_movie, id_person, id_movie)VALUES (\"${cast.credit_id}\", ${cast.cast_id}, \"${cast.character}\", ${cast.order}, ${cast.id_person}, ${cast.id_movie});"
    def writeSQLFileCastMovie(filePath: String, inserts: List[CastMovieTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCastMovie).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA CREW
  def generateInsertCrew(crew: CrewTable): String =
    s"INSERT INTO crew_miembros (id_person, name, gender, profile_path)VALUES (${crew.id_person}, \"${crew.name}\", ${crew.gender},CASE WHEN \"${crew.profile_path}\" = 'NULL' THEN NULL ELSE \"${crew.profile_path}\" END);"
  def writeSQLFileCrew(filePath: String, inserts: List[CrewTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCrew).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA JOBDEPARTMENT
  def generateInsertJobDepartment(job: JobDepartmentTable): String =
    s"INSERT INTO job_department (id_job, job, department)VALUES (${job.id_job}, \"${job.job}\", \"${job.department}\");"
  def writeSQLFileJobDepartment(filePath: String, inserts: List[JobDepartmentTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertJobDepartment).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  //ESCRIBIR ARCHIVO TABLA CREWMOVIE
  def generateInsertCrewMovie(crew: CrewMovieTable): String =
    s"INSERT INTO crew_movie (id_person, id_movie, credit_id, id_job)VALUES (${crew.id_person}, ${crew.id_movie}, \"${crew.credit_id}\", ${crew.id_job});"
  def writeSQLFileCrewMovie(filePath: String, inserts: List[CrewMovieTable]): IO[Unit] = {
    val sqlLines = inserts.map(generateInsertCrewMovie).mkString("\n")
    val path = Paths.get(filePath)
    IO(Files.write(path, sqlLines.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
      .void
  }

  /*
  FUNCIONES PARA POBLAR LAS TABLAS DIRECTAMENTE
   */


  // POBLAR TABLA COLLECTION
  def insertCollection(bel: CollectionTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO collection (id_collection, name, poster_path, backdrop_path)
     VALUES (
       ${bel.id_collection},
       ${bel.name},
       CASE WHEN ${bel.poster_path} = 'NULL' THEN NULL ELSE ${bel.poster_path} END,
       CASE WHEN ${bel.backdroop_path} = 'NULL' THEN NULL ELSE ${bel.backdroop_path} END
     )
   """.update.run
  }
  def insertAllCollection(bel: List[CollectionTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      bel.traverse(t => insertCollection(t).transact(xa))
    }
  }

  //POBLAR LA TABLA MOVIE
  def insertMovie(mov: MovieTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO movie (id_movie, title, original_title, imdb_id, budget, revenue, original_language,
                        overview, release_date, run_time, status, tagline, video, adult, popularity,
                        poster_path, homepage, vote_average, vote_count, id_collection)
     VALUES (
       ${mov.id_movie},
       ${mov.title},
       ${mov.original_title},
       ${mov.imdb_id},
       ${mov.budget},
       ${mov.revenue},
       CASE WHEN LENGTH(${mov.original_language}) > 2 THEN NULL ELSE ${mov.original_language} END,
       CASE WHEN ${mov.overview} = 'NULL' THEN NULL ELSE ${mov.overview} END,
       CASE WHEN ${mov.release_date} = 'NULL' THEN NULL ELSE ${mov.release_date} END,
       ${mov.run_time},
       ${mov.status},
       CASE WHEN ${mov.tagline} = 'NULL' THEN NULL ELSE ${mov.tagline} END,
       ${mov.video},
       ${mov.adult},
       ${mov.popularity},
       CASE WHEN ${mov.poster_path} = 'NULL' THEN NULL ELSE ${mov.poster_path} END,
       CASE WHEN ${mov.homepage} = 'NULL' THEN NULL ELSE ${mov.homepage} END,
       ${mov.vote_average},
       ${mov.vote_count},
       CASE WHEN ${mov.id_collection} = 0 THEN NULL ELSE ${mov.id_collection} END
      )
   """.update.run
  }
  def insertAllMovie(mov: List[MovieTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      mov.traverse(t => insertMovie(t).transact(xa))
    }
  }


  //POBLAR LA TABLA RATING
  def insertRating(rat: RatingTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO rating (rating_id, userId, rating, timestamp, id_movie)
     VALUES (
       ${rat.rating_id},
       ${rat.userId},
       ${rat.rating},
       ${rat.timestamp},
       ${rat.movie_id}
     )
   """.update.run
  }
  def insertAllRating(rat: List[RatingTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      rat.traverse(t => insertRating(t).transact(xa))
    }
  }


  //POBLAR LA TABLA LANGUAGE
  def insertLanguage(lan: LanguageTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO language (id_language, name)
     VALUES (
       ${lan.id_language},
       ${lan.name}
     )
   """.update.run
  }
  def insertAllLanguage(lan: List[LanguageTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      lan.traverse(t => insertLanguage(t).transact(xa))
    }
  }


  //POBLAR LA TABLA LANGUAGE_MOVIE
  def insertLanguageMovie(lan: LanguageMovieTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO language_movie (id_language, id_movie)
     VALUES (
       ${lan.id_language},
       ${lan.id_movie}
     )
   """.update.run
  }
  def insertAllLanguageMovie(lan: List[LanguageMovieTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      lan.traverse(t => insertLanguageMovie(t).transact(xa))
    }
  }

  //POBLAR LA TABLA KEYWORD
  def insertKeyword(key: KeywordTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO keyword (id_keyword, name)
     VALUES (
       ${key.id_keyword},
       ${key.name}
     )
   """.update.run
  }
  def insertAllKeyword(key: List[KeywordTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      key.traverse(t => insertKeyword(t).transact(xa))
    }
  }

  //POBLAR LA TABLA KEYWORD_MOVIE
  def insertKeywordMovie(key: KeywordMovieTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO keyword_movie (id_keyword, id_movie)
     VALUES (
       ${key.id_keyword},
       ${key.id_movie}
     )
   """.update.run
  }
  def insertAllKeywordMovie(key: List[KeywordMovieTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      key.traverse(t => insertKeywordMovie(t).transact(xa))
    }
  }

  //POBLAR LA TABLA GENDER
  def insertGender(gen: GenderTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO gender (id_gender, name)
     VALUES (
       ${gen.id_gender},
       ${gen.name}
     )
   """.update.run
  }
  def insertAllGender(gen: List[GenderTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      gen.traverse(t => insertGender(t).transact(xa))
    }
  }

  //POBLAR LA TABLA GENDER_MOVIE
  def insertGenderMovie(gen: GenderMovieTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO gender_movie (id_gender, id_movie)
     VALUES (
       ${gen.id_gender},
       ${gen.id_movie}
     )
   """.update.run
  }
  def insertAllGenderMovie(gen: List[GenderMovieTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      gen.traverse(t => insertGenderMovie(t).transact(xa))
    }
  }

  //POBLAR LA TABLA COUNTRY
  def insertCountry(coun: CountryTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO country (id_country, name)
     VALUES (
       ${coun.id_country},
       ${coun.name}
     )
   """.update.run
  }
  def insertAllCountry(coun: List[CountryTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      coun.traverse(t => insertCountry(t).transact(xa))
    }
  }

  //POBLAR LA TABLA COUNTRY_MOVIE
  def insertCountryMovie(coun: CountryMovieTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO country_movie (id_country, id_movie)
     VALUES (
       ${coun.id_country},
       ${coun.id_movie}
     )
   """.update.run
  }
  def insertAllCountryMovie(coun: List[CountryMovieTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      coun.traverse(t => insertCountryMovie(t).transact(xa))
    }
  }

  //POBLAR LA TABLA COMPANY
  def insertCompany(com: CompanyTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO company (id_company, name)
     VALUES (
       ${com.id_company},
       ${com.name}
     )
   """.update.run
  }
  def insertAllCompany(com: List[CompanyTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      com.traverse(t => insertCompany(t).transact(xa))
    }
  }

  //POBLAR LA TABLA COMPANY_MOVIE
  def insertCompanyMovie(com: CompanyMovieTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO company_movie (id_company, id_movie)
     VALUES (
       ${com.id_company},
       ${com.id_movie}
     )
   """.update.run
  }
  def insertAllCompanyMovie(com: List[CompanyMovieTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      com.traverse(t => insertCompanyMovie(t).transact(xa))
    }
  }

  //POBLAR LA TABLA CAST
  def insertCast(cast: CastTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO cast_miembros (id_person, name, gender, profile_path)
     VALUES (
       ${cast.id_person},
       ${cast.name},
       ${cast.gender},
       CASE WHEN ${cast.profile_path} = 'NULL' THEN NULL ELSE ${cast.profile_path} END
     )
   """.update.run
  }
  def insertAllCast(cast: List[CastTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      cast.traverse(t => insertCast(t).transact(xa))
    }
  }

  //POBLAR LA TABLA CAST_MOVIE
  def insertCastMovie(cast: CastMovieTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO cast_movie (credit_id, cast_id, character_movie, order_movie, id_person, id_movie)
     VALUES (
       ${cast.credit_id},
       ${cast.cast_id},
       ${cast.character},
       ${cast.order},
       ${cast.id_person},
       ${cast.id_movie}
     )
   """.update.run
  }
  def insertAllCastMovie(cast: List[CastMovieTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      cast.traverse(t => insertCastMovie(t).transact(xa))
    }
  }

  //POBLAR LA TABLA CREW
  def insertCrew(crew: CrewTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO crew_miembros (id_person, name, gender, profile_path)
     VALUES (
       ${crew.id_person},
       ${crew.name},
       ${crew.gender},
       CASE WHEN ${crew.profile_path} = 'NULL' THEN NULL ELSE ${crew.profile_path} END
     )
   """.update.run
  }
  def insertAllCrew(crew: List[CrewTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      crew.traverse(t => insertCrew(t).transact(xa))
    }
  }

  //POBLAR LA TABLA JOB_DEPARTMENT
  def insertJobDepartment(job: JobDepartmentTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO job_department (id_job, job, department)
     VALUES (
       ${job.id_job},
       ${job.job},
       ${job.department}
     )
   """.update.run
  }
  def insertAllJobDepartment(job: List[JobDepartmentTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      job.traverse(t => insertJobDepartment(t).transact(xa))
    }
  }

  //POBLAR LA TABLA JOB_DEPARTMENT
  def insertCrewMovie(crew: CrewMovieTable): ConnectionIO[Int] = {
    sql"""
     INSERT INTO crew_movie (id_person, id_movie, credit_id, id_job)
     VALUES (
       ${crew.id_person},
       ${crew.id_movie},
       ${crew.credit_id},
       ${crew.id_job}
     )
   """.update.run
  }
  def insertAllCrewMovie(crew: List[CrewMovieTable]): IO[List[Int]] = {
    movieDBConfig.transactor.use { xa =>
      crew.traverse(t => insertCrewMovie(t).transact(xa))
    }
  }

  //QUERYS PARA CRUD
  // CONSULTAR UNA PELICULA POR SU ID
  def searchMovieById(id : Int): IO[Option[(Int, String, String, String)]] = {
    movieDBConfig.transactor.use { xa =>
      sql"""SELECT id_movie, original_title, release_date, original_language
        FROM Movie
        WHERE id_movie = $id"""
        .query[(Int, String, String, String)]
        .option
        .transact(xa)
    }
  }

  // CONSULTAR UNA PELICULA POR SU PAIS
  def searchMovieByCountry(country : String): IO[List[(Int, String, String, String, String)]] = {
    movieDBConfig.transactor.use { xa =>
      sql"""SELECT m.id_movie, m.original_title, m.release_date, m.original_language, c.name
            FROM movie m
            JOIN country_movie cm ON m.id_movie = cm.id_movie
            JOIN country c ON cm.id_country = c.id_country
            WHERE c.name = $country"""
        .query[(Int, String, String, String, String)]
        .to[List]
        .transact(xa)
    }
  }

  // CONSULTAR UNA PELICULA POR SU AÑO
  def searchMovieByYear(year : Int): IO[List[(Int, String, Int)]] = {
    movieDBConfig.transactor.use { xa =>
      sql"""SELECT id_movie, original_title, YEAR(release_date)
        FROM Movie
        WHERE YEAR(release_date) = $year"""
        .query[(Int, String, Int)]
        .to[List]
        .transact(xa)
    }
  }

  // BUSCAR UNA PELICULA QUE SUPERE N PRESUPUESTO
  def searchMovieByBudget(budget : Int): IO[List[(Int, String, Int)]] = {
    movieDBConfig.transactor.use { xa =>
      sql"""SELECT id_movie, original_title, budget
        FROM Movie
        WHERE budget >= $budget"""
        .query[(Int, String, Int)]
        .to[List]
        .transact(xa)
    }
  }

  // BUSCAR PELICULAS POR UNA KEYWORD ESPECIFICA
  def searchMovieByKeyword(keyword : String): IO[List[(Int, String, String, String, String)]] = {
    movieDBConfig.transactor.use { xa =>
      sql"""SELECT m.id_movie, m.original_title, m.release_date, m.original_language, k.name
        FROM movie m
        JOIN keyword_movie km ON m.id_movie = km.id_movie
        JOIN keyword k ON km.id_keyword = k.id_keyword
        WHERE k.name = $keyword"""
        .query[(Int, String, String, String, String)]
        .to[List]
        .transact(xa)
    }
  }
}