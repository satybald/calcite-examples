name := "Calcite-Foodmart"

version := "1.0"
scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.apache.calcite" % "calcite" % "1.16.0",
  "org.apache.calcite" % "calcite-core" % "1.16.0",
  "org.postgresql" % "postgresql" % "42.2.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3")

resolvers += Resolver.mavenLocal