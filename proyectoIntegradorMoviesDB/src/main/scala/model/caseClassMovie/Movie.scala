package model.caseClassMovie

case class Movie(
                  adult: String, // BOOLEAN
                  belongs_to_collection: String, // JSON
                  budget: Int,
                  genres: String, // JSON
                  homepage: String,
                  id: Int,
                  imdb_id: String,
                  original_language: String,
                  original_title: String,
                  overview: String,
                  popularity: Double,
                  poster_path: String,
                  production_companies: String, // JSON
                  production_countries: String, // JSON
                  release_date: String,
                  revenue: Long,
                  runtime: Int,
                  spoken_languages: String, // JSON
                  status: String,
                  tagline: String,
                  title: String,
                  video: String, // BOOLEAN
                  vote_average: Double,
                  vote_count: Int,
                  keywords: String, // JSON
                  cast: String, // JSON
                  crew: String, // JSON
                  ratings: String // JSON
                      )