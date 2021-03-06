name := "phoenix-spark-toolkit"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "com.github.mjakubowski84" %% "parquet4s-core" % "1.7.0",
  "org.apache.hadoop" % "hadoop-client" % "3.2.0",
  "org.apache.phoenix" % "phoenix" % "4.14.0-HBase-1.3",
  "org.apache.phoenix" % "phoenix-spark" % "4.14.0-HBase-1.3",
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "com.jsuereth" %% "scala-arm" % "2.0",
  "com.google.protobuf" % "protobuf-java" % "2.5.0",
  "org.scalatest"            %% "scalatest"       % "3.1.2" % "test"
)