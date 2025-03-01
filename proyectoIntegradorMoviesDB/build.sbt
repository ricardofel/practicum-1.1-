ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "proyectoIntegrador_ejecutable",
    libraryDependencies ++= Seq(
      "io.reactivex" %% "rxscala" % "0.27.0",
      "com.lihaoyi" %% "scalarx" % "0.4.1",
      "com.nrinaudo" %% "kantan.csv" % "0.6.1",
      "com.nrinaudo" %% "kantan.csv-generic" % "0.6.1",
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC5",      // Dependencias de doobie
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC5",    // Para gestión de conexiones
      "com.mysql" % "mysql-connector-j" % "8.0.31",       // Driver para MySQL
      "com.typesafe" % "config"           % "1.4.2",      // Para gestión de archivos de configuración
      "ch.qos.logback" % "logback-classic" % "1.2.3"
    )
  )