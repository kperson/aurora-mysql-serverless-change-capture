val scalaTestVersion = "3.0.5"
val scalaMockSupportVersion = "3.6.0"

lazy val commonSettings = Seq(
  organization := "com.github.kperson",
  version := "1.0.0",
  scalaVersion := "2.12.8",

  parallelExecution in Test := false
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
   fork in run := true,
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("reference.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  ).
  settings(libraryDependencies ++= Seq (
    "mysql"                   % "mysql-connector-java"        % "8.0.14",
    "org.slf4j"               % "slf4j-api"                   % "1.7.25",
    "ch.qos.logback"          %  "logback-classic"            % "1.2.3" % "runtime"
  ))