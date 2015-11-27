name := "loggerhead"

organization := ""

version := "0.0.1"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "hortonworks" at "http://repo.hortonworks.com/content/repositories/releases/",
  "hortonpublic" at "http://repo.hortonworks.com/content/groups/public/"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.0.M5b" % "test" withSources() withJavadoc(),
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-core" % "1.4.1" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-hive" % "1.4.1" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.1",
//  "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided" withSources() withJavadoc(),
//  "org.apache.spark" %% "spark-graphx" % "1.3.1" % "provided" withSources() withJavadoc(),
//  "org.apache.hadoop" % "hadoop-client" % "2.7.1.2.3.2.8-1",
  "io.argonaut" %% "argonaut" % "6.0.4",
  "com.github.scopt" %% "scopt" % "3.3.0"
)

//enablePlugins(JavaAppPackaging)
//enablePlugins(UniversalPlugin)

initialCommands := "import .loggerhead._"

