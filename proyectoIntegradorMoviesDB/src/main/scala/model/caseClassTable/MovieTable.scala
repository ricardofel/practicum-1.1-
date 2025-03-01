package model.caseClassTable

case class MovieTable(
                      id_movie: Int,
                      title: String,
                      original_title: String,
                      imdb_id: String,
                      budget: Long,
                      revenue: Double,
                      original_language: String,
                      overview: String,
                      release_date: String,
                      run_time: Int,
                      status: String,
                      tagline: String,
                      video: Int,
                      adult: Int,
                      popularity: Double,
                      poster_path: String,
                      homepage: String,
                      vote_average: Double,
                      vote_count: Int,
                      id_collection: Int
                      )