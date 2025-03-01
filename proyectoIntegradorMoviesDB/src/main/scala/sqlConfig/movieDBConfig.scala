// AUTOR: RICARDO E.
package sqlConfig

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import com.typesafe.config.ConfigFactory

import doobie.hikari.HikariTransactor

import scala.concurrent.ExecutionContext

object movieDBConfig {
  private val connectEC: ExecutionContext = ExecutionContext.global

  def transactor: Resource[IO, HikariTransactor[IO]] = {
    val config = ConfigFactory.load().getConfig("db")
    HikariTransactor.newHikariTransactor[
      IO
    ](
      config.getString("driver"),
      config.getString("url"),
      config.getString("user"),
      config.getString("password"),
      connectEC
    )
  }
}