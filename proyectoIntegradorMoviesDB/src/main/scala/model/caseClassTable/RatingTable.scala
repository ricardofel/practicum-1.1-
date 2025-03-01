package model.caseClassTable

case class RatingTable(
                   rating_id: Int,
                   userId: Int,
                   rating: Double,
                   timestamp: Long,
                   movie_id: Int
                 )