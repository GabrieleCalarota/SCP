package foodReview.operations

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.{URI, URL}
import java.util.zip.GZIPInputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}
import org.joda.time.DateTime

import scala.io.Source

class InvalidOp(s:String) extends Exception(s) {}


object Utils {
  /**
    * Initializes 'spark' by adding all the required udf.
    *
    * @param spark SparkSession context.
    */
  //TODO: remove this
  def initSession(spark: SparkSession): Unit = {
    spark.udf.register("year", (t: String) => {
      val dt = new DateTime(t.toLong * 1000)
      dt.getYear.toString
    })
    spark.udf.register("month", (t: String) => {
      val dt = new DateTime(t.toLong * 1000)
      dt.getMonthOfYear.toString
    })
    spark.udf.register("square", (v: Double, avg: Double) => (v - avg) * (v - avg))
    spark.udf.register("product", (v1: Double, v2: Double, avg1: Double, avg2: Double) => (v1 - avg1) * (v2 - avg2))
    spark.udf.register("bayes", (avg: Double, cnt: Double, avgTot: Double, minVotes: Int) =>
      (avg * cnt + avgTot * minVotes) / (cnt + minVotes))
  }

  /**
    * Computes the df (userId, score of 'product').
    *
    * @param df DataFrame with at least (userId, productId, score).
    * @param product Product identifier.
    * @return DataFrame with schema (userId, score).
    */
  def productScore(df: DataFrame, product: String): DataFrame = {
    df.where(s"productId = '$product'").select("userId", "score")
  }

  /**
    * Computes the df of all the distinct products in 'df'.
    *
    * @param df DataFrame with at least (productId).
    * @return DataFrame with schema (productId).
    */
  def distinctProducts(df: DataFrame): DataFrame = {
    df.select("productId").distinct()
  }

  /**
    * Computes the df of all the distinct users in 'df'.
    *
    * @param df DataFrame with at least (userId).
    * @return DataFrame with schema (userId).
    */
  def distinctUsers(df: DataFrame): DataFrame = {
    df.select("userId").distinct()
  }


  /**
    * Divides the element of 'm1' for the corresponding element of 'm2'.
    *
    * @param m1 Map String->Double.
    * @param m2 Map String->Double, with the same keys of 'm1'.
    * @return Map String->Double.
    */
  def divideMaps(m1: Map[String, Double], m2: Map[String, Double]): Map[String, Double] = {
    m1.map(r => (r._1, r._2 / m2(r._1)))
  }

  /**
    * Computes the df (group identifier, average score of the group identifier).
    *
    * @param df DataFrame (userId, productId, score).
    * @param group Column for aggregate.
    * @param cnt Column for count.
    * @return DataFrame ('group', avgScore).
    */
  def avgGroupScore(df: DataFrame, group: String = "userId", cnt: String = "productId"): DataFrame = {
    df.groupBy(group).agg(sum("score") / count(cnt) as "avgScore")
  }

  /**
    * Computes the average of the scores over the whole DataFrame.
    *
    * @param df DataFrame with at least (score).
    * @return The average of the scores.
    */
  def avgScore(df: DataFrame): Double = {
    df.agg(sum("score") / count("score") as "avgScore").first().get(0).toString.toDouble
  }

  /**
    * Computes the intersection between 'm1' and 'm2', where the keys are the common items and the values are the
    * corresponding sums.
    *
    * @param m1 Map String->Double.
    * @param m2 Map String->Double.
    * @return Map String->Double.
    */
  def intersect(m1: Map[String, Double], m2: Map[String, Double]): Map[String, Double] = {
    val k1 = Set(m1.keysIterator.toList: _*)
    val k2 = Set(m2.keysIterator.toList: _*)
    val intersection = k1 & k2
    val r1: Set[(String, Double)] = for (key <- intersection) yield key -> (m1(key) + m2(key))
    r1.toMap
  }
  /**
    * Computes the union between 'm1' and 'm2'
    * @note For common keys, the values are the sums of the two values.
    *
    * @param m1 Map String->Double.
    * @param m2 Map String->Double.
    * @return Map String->Double.
    */
  def union(m1: Map[String, Double], m2: Map[String, Double]): Map[String, Double] = {
    val k1 = Set(m1.keysIterator.toList: _*)
    val k2 = Set(m2.keysIterator.toList: _*)
    val intersection = k1 & k2
    val r1: Set[(String, Double)] = for (key <- intersection) yield key -> (m1(key) + m2(key))
    val r2 = m1.filterKeys(!intersection.contains(_)) ++ m2.filterKeys(!intersection.contains(_))
    r2 ++ r1
  }

  /**
    * Checks if an id exists as 'col' of 'rumRDD'.
    *
    * @param rumRDD RDD containing at least 'col'.
    * @param id Identifier.
    * @param key Key of the RDD.
    * @return Boolean true if the 'rumRDD' contains 'id', false otherwise.
    */
  def idExists(rumRDD:RDD[Map[String,String]], id:String, key:String): Boolean = {
    try {
      rumRDD.filter(r => r.getOrElse(key, "-1").equals(id)).count() > 0
    } catch {
      case _:AnalysisException => false
    }
  }

  /**
    * Checks if an id exists in the column 'col' of 'rumDf'.
    *
    * @param rumDf DataFrame with at least 'col'.
    * @param id Identifier.
    * @param colId Column identifier.
    * @return Boolean true if the 'rumDf' contains 'id', false otherwise.
    */
  def idExists(rumDf: DataFrame, id: String, colId:String): Boolean = {
    try {
      !rumDf.where(s"$colId = '$id'").isEmpty
    } catch {
      case _:AnalysisException => false
    }
  }

  /**
    * Gets product name by productId.
    *
    * @param df DataFrame (productId, name).
    * @param productId Required product identifier.
    * @return Name of 'productId'.
    * @throws InvalidOp if 'productId' doesn't exist.
    */
  def getProductName(df:DataFrame, productId:String): String = {
    if (!idExists(df, productId, "productId"))
      throw new InvalidOp("Not existing id")
    df.where(s"productId = '$productId'").first().get(1).toString
  }

  /**
    *  Download file into filepath
    *
    *  @param url Url of the file
    *  @param filename Path location of the file to be saved
    *
    *  @return Unit
   */
  def fileDownloader(url: String, filename: String): Unit = {
    import sys.process._
    new URL(url) #> new File(filename) !!
  }

  /**
    * Create directory path
    *
    * @param List of path to create
    * @return True if path was created
   */
  def mkdirs(path: List[String]) :Boolean =
    path.tail.foldLeft(new File(path.head)){(a,b) => a.mkdir; new File(a,b)}.mkdir

  /**
    * Download dataset and unzip in resource directory
    *
    * @param url Dataset url at the time of writing the project
    * @param path default=.resource Location to save the dataset downloaded
    * @return Unit
    * @throws Exception if something goes wrong downloading or unpacking the dataset
   */
  def downloadAndSaveDataset(sparkSession: SparkSession, url:String="https://snap.stanford.edu/data/finefoods.txt.gz", path:String="resources/"): String = {
    val gzName:String = "finefoods.txt.gz"
    val datasetName:String = "dataset.txt"
    val fs = FileSystem.get(URI.create(path), sparkSession.sparkContext.hadoopConfiguration)
    val fileExists = fs.exists(new Path(path+datasetName))
    if (fileExists){
      path+datasetName
    } else {
      fs.mkdirs(new Path(path))
      println("Downloading dataset ..")
      fileDownloader(url, path + gzName)
      /*sparkSession.sparkContext.addFile(url)
      val df: DataFrame = sparkSession.read.text(SparkFiles.get(gzName))
      df.write.save(path+datasetName)*/
      println("Unpacking gzip: "+path+gzName)
      var in = new GZIPInputStream(new FileInputStream(path + gzName))
      var fos = new FileOutputStream(path + datasetName)
      for (line <- Source.fromInputStream(in)("ISO-8859-1").getLines()) {
        fos.write((line + "\n").getBytes())
      }
      fos.close()
      println("Clean up ..")
      new File(path + gzName).delete()
      println("Dataset saved at: "+path+datasetName)
      path + datasetName
    }
  }

  /**
    * Open read-mode file
    *
    * @param spark SparkSession.
    * @param path where the file is located.
    * @return the file.
    */
  def openFile(spark:SparkSession, path:String): Dataset[String] = {
    spark.read.textFile(path)
  }

  /**
    * Transform bytes human-readable memory size
    * @param numBytes number of bytes to transform
    * @return String of number transformed and unit of measure
   */
  def transformBytes(numBytes:Long, unit: String="b"): String = {
    if (numBytes >= 1024){
      transformBytes(numBytes/1024, unit=getNextUnitMemorySize(unit))
    } else {
      s"$numBytes $unit"
    }
  }

  /**
    * Get the next memory size unit available
    *
    * @param unit string denoting memory size
    * @return String denoting next memory size
    */
  def getNextUnitMemorySize(unit: String): String = {
    val states = Map("b" -> "KB", "KB" -> "MB", "MB" -> "GB", "GB" -> "TB")
    states(unit)
  }

}
