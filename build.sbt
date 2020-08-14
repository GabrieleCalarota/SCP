name := "FoodReview"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "FoodReview.jar"
assemblyOutputPath in assembly := file("./FoodReview.jar")