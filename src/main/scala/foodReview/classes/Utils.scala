package foodReview.classes

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.{URI, URL}
import java.util.zip.GZIPInputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}

import scala.io.Source

class InvalidOp(s:String) extends Exception(s) {}


object Utils {

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
    * @param path List of path to create
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

  /**
    * Evaluates and print elapsed time of a block
    *
    * @param block instructions to be executed
    * @return Double of seconds elapsed
  */
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + "s")
    //(t1 - t0)/1000000000.0
    result
  }
}
