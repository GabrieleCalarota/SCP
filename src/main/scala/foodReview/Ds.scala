package foodReview

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.math.min
import scala.util.Random


object Ds {

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
    * Gets dataset from txt as DataFrame.
    *
    * @param spark SparkSession.
    * @param path Path of the txt.
    * @return DataFrame corresponding to the given txt.
    */
  //TODO: remove
  /*def importStanfordDF(spark:SparkSession, path:String): DataFrame = {
    import spark.implicits._
    val s = importRDD(spark, path).map(m => {(
      m.getOrElse("userId", ""),
      m.getOrElse("productId", ""),
      m.getOrElse("score", ""),
      m.getOrElse("time", ""),
      m.getOrElse("helpfulness", ""))
    }).toDF()
      .withColumnRenamed("_1", "userId")
      .withColumnRenamed("_2", "productId")
      .withColumnRenamed("_3", "score")
      .withColumnRenamed("_4", "time")
      .withColumnRenamed("_5", "helpfulness")
    s
  }*/

  /**
    * Doubles the size of 'rumDf' with random userIds and productIds.
    * Function useful for stress testing.
    *
    * @param rumDf DataFrame.
    * @return DataFrame with the same schema of 'rumDf' but with double size.
    */
  //TODO: convert to RDD
  def increaseSizeDf(rumDf:DataFrame): DataFrame = {
    val df = rumDf
    val maxProducts = rumDf.select("productId").agg(max("productId")).head.get(0).toString.toInt
    val maxUser = rumDf.select("userId").agg(max("userId")).head.get(0).toString.toInt
    df.withColumn("productId", col("productId").cast("Int") + Random.nextInt(maxProducts))
      .withColumn("userId", col("userId").cast("Int") + Random.nextInt(maxUser))
      .union(rumDf)
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
  //TODO: STORE for RDD?
  def storeDfPt1(df:DataFrame, path:String, partition:Int=1):Unit = {
    df.repartition(partition)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(path)
  }
}
