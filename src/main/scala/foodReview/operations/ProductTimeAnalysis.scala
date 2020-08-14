package foodReview.operations

import foodReview.operations.Utils.idExists
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object ProductTimeAnalysis {

  /**
    * Computes the DataFrame (productId, year, average score of the year) from 'yBegin' to 'yEnd'.
    *
    * @param tsDf DataFrame with at least (score, timestamp).
    * @param yBegin First year of the interval (if given, otherwise first year of a score for the product).
    * @param yEnd Last year of the interval (if given, otherwise last year of a score for the product).
    * @return DataFrame (productId, year, avgScore).
    */
  //TODO: remove
  /*private def yearDF(tsDf:DataFrame, yBegin:String=null, yEnd:String=null): DataFrame = {
    val scoreDf = tsDf.select(
      col("productId"),
      col("score"),
      callUDF("year", col("time")).cast("int") as "year"
    )
    val scoreBeginDf = yBegin match {
      case null => scoreDf
      case _ => scoreDf.where(s"year >=  $yBegin")
    }
    val scoreEndDf = yEnd match {
      case null => scoreBeginDf
      case _ => scoreBeginDf.where(s"year <= $yEnd")
    }
    scoreEndDf.groupBy("productId", "year")
      .agg(sum("score")/count("score") as "avgScore")
  }*/

  /**
    * Computes the DataFrame (productId, month of 'year', avgScore).
    *
    * @param tsDf DataFrame with at least (score, time).
    * @param year Year.
    * @return DataFrame (productId, month of 'year', avgScore).
    */
  //TODO: remove
  /*private def monthDF(tsDf:DataFrame, year:String): DataFrame = {
    val scoreDf = tsDf.select(
      col("productId"),
      col("score"),
      col("time"),
      callUDF("year", col("time")) as "year"
    ).where(s"year = $year")
    scoreDf.select(
      col("productId"),
      col("score"),
      callUDF("month", col("time")).cast("int") as "month"
    ).groupBy("productId", "month").agg(sum("score")/count("score") as "avgScore")
  }*/

  /**
    * Computes a DF containing data about the evolution of the average score for the given products.
    *
    * @note Data analysis can be performed either in a given interval of years by year
    *       or in a given year by month.
    *
    * @param rumDf DataFrame with at least (productId, score, time).
    * @param productId Identifier of a product.
    * @param productIds Identifier of product(s).
    * @param byMonth Boolean true if the analysis is by month in the given year, false if it's by year.
    * @param yBegin First year of the interval, if 'byMonth' is false, year of the analysis otherwise.
    * @param yEnd Last year of the interval, optional if 'byMonth' is false.
    * @return DataFrame of time evolution.
    * @throws InvalidOp if one of the productIds (product + products) doesn't exist.
    */
  //TODO: remove
  /*def timeDFD(rumDf: DataFrame, productId:String, productIds: String*)
             (byMonth: Boolean=false, yBegin: String=null, yEnd: String=null): DataFrame = {
    (productId+: productIds).foreach(mId => {
      if(!idExists(rumDf, mId, "productId"))
        throw new InvalidOp("Not existing id " + mId)
    })
    val whereCond = (productId +: productIds).map(x => s"productId = '$x'").reduce((a, b) => s"$a or $b" )
    val usefulDf = rumDf.select("productId", "time", "score").where(whereCond)
    if (byMonth) monthDF(usefulDf, yBegin) else yearDF(usefulDf, yBegin, yEnd)
  }*/

  /**
    * Computes the RDD (productId, year, average score of the year).
    *
    * @param tsRDD RDD of Maps with keys (score, time).
    * @param yBegin First year of the interval (if given, otherwise first year of the product).
    * @param yEnd Last year of the interval (if given, otherwise last year of the product).
    * @return RDD (productId, year, average score of the year).
    */
  private def yearRDD(tsRDD:RDD[Map[String,String]], yBegin:String=null, yEnd:String=null): RDD[(String, Int, Double)] = {
    // RDD ((year, Id), (score, 1 for cnt))
    val scoreRDD = tsRDD.map(r=>{
      val dt = new DateTime(r.getOrElse("time","0").toLong * 1000)
      ((r.getOrElse("productId", "-1"), dt.getYear.toString.toInt),(r.getOrElse("score","0.0").toDouble,1))
    })
    val scoreBeginRDD = yBegin match {
      case null => scoreRDD
      case _ => scoreRDD.filter(_._1._2 >= yBegin.toInt)
    }
    val scoreEndRDD = yEnd match {
      case null => scoreBeginRDD
      case _ => scoreRDD.filter(_._1._2 <= yEnd.toInt)
    }
    // RDD(Id, year, avgScore)
    scoreEndRDD.reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2)).map(y=>(y._1._1, y._1._2,y._2._1/y._2._2))
  }

  /**
    * Computes the RDD (productId, month of 'year', avgScore).
    *
    * @param tsRDD RDD of Maps with keys (score, time).
    * @param year Year.
    * @return RDD (productId, month of 'year', avgScore).
    */
  private def monthRDD(tsRDD:RDD[Map[String,String]], year:String): RDD[(String, Int, Double)]  = {
    // RDD ((Id, month), (score, 1 for cnt))
    val scoreRDD = tsRDD.map(r=>{
      val dt = new DateTime(r.getOrElse("time","0").toLong * 1000)
      (dt.getYear.toString.toInt,
      (r.getOrElse("productId", "-1"), dt.getMonthOfYear.toString.toInt, r.getOrElse("score","0.0").toDouble,1))
    })
    .filter(_._1==year.toInt).map(m=>((m._2._1, m._2._2),(m._2._3, m._2._4)))
    scoreRDD
      // RDD (sum score, cnt)
      .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
      // RDD (Id, month, avgScore)
      .map(y=>(y._1._1, y._1._2, y._2._1/y._2._2))
    }


  /**
    * Computes a DF containing data about the evolution of the average score for the given products.
    *
    * @note Data analysis can be performed either in a given interval of years by year
    *       or in a given year by month.
    *
    * @param spark Spark context for the conversion from RDD to DF
    * @param rumRDD RDD of Maps with keys (productId, score, time).
    * @param productId Identifier of a product.
    * @param productIds Identifier of product(s).
    * @param byMonth Boolean true if the analysis is by month in the given year, false if it's by year.
    * @param yBegin First year of the interval, if 'byMonth' is false, year of the analysis otherwise.
    * @param yEnd Last year of the interval, optional if 'byMonth' is false.
    * @return DataFrame of time evolution.
    * @throws InvalidOp if one of the productIds (product + products) doesn't exist.
    */
  def timeDFR(spark: SparkSession, rumRDD: RDD[Map[String,String]], productId:String, productIds: String*)
             (byMonth: Boolean=false, yBegin: String=null, yEnd: String=null): DataFrame = {
    (productId+: productIds).foreach(mId => {
      if(!idExists(rumRDD, mId, "productId"))
        throw new InvalidOp("Not existing id " + mId)
    })
    val mRDD = rumRDD.filter(r=> (productId +: productIds).contains(r.getOrElse("productId", "-1")))
    val (rdd, axis) = if (byMonth) {
        (monthRDD(mRDD, yBegin), "month")
      } else {
        (yearRDD(mRDD, yBegin, yEnd), "year")
      }
    spark.createDataFrame(rdd).toDF("productId", axis, "avgScore")
  }
}
