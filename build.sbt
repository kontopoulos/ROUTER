name := "GLASSEAS"

version := "0.1"

scalaVersion := "2.12.8"


libraryDependencies += "de.lmu.ifi.dbs.elki" % "elki" % "0.7.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"

libraryDependencies += "com.typesafe.akka" %% "akka-cluster-sharding-typed" % "2.5.16"

libraryDependencies += "com.typesafe.akka" %% "akka-typed" % "2.5.8"
