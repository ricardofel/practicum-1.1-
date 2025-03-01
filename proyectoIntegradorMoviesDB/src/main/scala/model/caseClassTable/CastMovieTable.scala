package model.caseClassTable

case class CastMovieTable(
                           credit_id: String,
                           cast_id: Int,
                           character: String,
                           order: Int,
                           id_person: Long,
                           id_movie: Int
                           )
