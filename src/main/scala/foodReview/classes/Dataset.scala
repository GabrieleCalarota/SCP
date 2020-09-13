package foodReview.classes

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.min

object Dataset {

  /**
    * Converts standford txt in RDD of Maps where each Map has "userId", "productId", "score" "time" and "helpfulness"
    * as keys.
    *
    * @param spark SparkSession.
    * @param path Path of the txt.
    * @return RDD of Maps.
    */
  def importRDD(spark:SparkSession, path:String): RDD[Map[String, String]] = {
    val conf = new org.apache.hadoop.mapreduce.Job().getConfiguration
    conf.set("textinputformat.record.delimiter", "\n\n")
    val lines = spark.sparkContext
      .newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(_._2.toString)
    val res: RDD[Map[String, String]] = lines.map(b => {
      val blockMap = Map[String, String]()
      var r1 = Map[String, String]()
      r1 = b.split("\n").foldLeft(blockMap)((bm:Map[String, String], line:String) => {
        try{
          val tmp = line.split(':')
          val genericId = tmp(0).split('/')(1)
          var value = ""
          val tmpVal = tmp.slice(1, tmp.length).foldLeft("")((a, b) => a+b.trim)
          value =  genericId match {
              case "helpfulness" => if (tmpVal.split("/")(1).toInt == 0) "0" else min(tmpVal.split("/")(0).toInt * 100 / tmpVal.split("/")(1).toInt, 100).toString
              case _ => tmpVal
            }
          genericId match {
            case "userId" | "productId" | "score" | "time" | "helpfulness" => bm.updated(genericId, value)
            case _ => bm
          }
        } catch {
          case _:Exception => bm
        }
    })
      r1
    }).filter(m => Set("userId", "productId", "score", "time", "helpfulness") == m.keySet)
    res
  }

  /**
    * Doubles the size of dataset with random userIds and productIds.
    * Function useful for stress testing.
    *
    * @param rdd dataset.
    * @return RDD with the same schema of dataset but with size larger.
    */
  def increaseSizeDf(sparkContext: SparkContext, rdd:RDD[Map[String, String]], size: Double=2.0): RDD[Map[String, String]] = {
    var fraction: Double = 1
    val bigRdd: RDD[Map[String, String]] = if (size > 1) {
      val newRdds = (1 to size.toInt).map(_ => rdd)
      val bigRdd = sparkContext.union(newRdds)
      bigRdd
    } else {
      fraction = size
      rdd
    }
    val sampleRdd: RDD[Map[String, String]] = bigRdd.sample(withReplacement = false, fraction)
    sampleRdd
  }

  /**
    * Stores the input DataFrame as a csv file in 'path'.
    * 'path' should include the csv extension.
    * @note number of partitions is fixed to 1.
    *       If file exists, it's overwritten
    *
    * @param df DataFrame to store.
    * @param path Path storage location.
    */
  def storeDfPt1(df:DataFrame, path:String, partition:Int=1):Unit = {
    df.repartition(partition)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(path)
  }
}
