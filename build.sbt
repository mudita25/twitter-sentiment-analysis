import AssemblyKeys._

name := "twitter-streaming"
//name := "Twitter Streaming Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
   "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided",
   "org.apache.spark" %% "spark-streaming-twitter" % "1.4.1",
   "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided"
)

//assemblyJarName in assembly := "twitter-streaming-assembly.jar"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

