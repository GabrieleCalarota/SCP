package foodReview.operations

import org.apache.spark.rdd.RDD

class ProductRanking {

  protected var _minVotes = 150

  /**
    * Computes the df (productId, #users that watched the product, average of the product).
    *
    * @param df DataFrame with at least (productId, userId, score).
    * @return DataFrame with schema (productId, count, avgScore).
    */
  //TODO: REMOVE
  /*private def cntAvgByProduct(df: DataFrame): DataFrame = {
    df.groupBy("productId").agg(count("userId") as "count",
      sum("score") / count("userId") as "avgScore")
  }*/

  /**
    * Computes a true bayesian estimate on 'rumDf', to obtain the product rank over the whole DataFrame.
    *
    * @param rumDf DataFrame with at least (productId, userId, score).
    * @return DataFrame with schema (productId, score).
    */
  //TODO: remove
  /*def trueBayesianEstimate(rumDf:DataFrame): DataFrame = {
    val ascDf:DataFrame = cntAvgByProduct(rumDf)
    //println(ascDf)
    val avgTot:Double = avgScore(rumDf)
    ascDf.select(
      col("productId"),
      callUDF("bayes", col("avgScore"), col("count"), lit(avgTot), lit(_minVotes)) as "score")
  }*/

  /**
    * Computes a true bayesian estimate on 'rumRDD', to obtain the product rank over the whole RDD.
    *
    * @param rumRDD RDD where each Map has the keys productId, userId, score.
    * @return RDD of (productId, score).
    */
  def trueBayesianEstimate(rumRDD: RDD[Map[String,String]]): RDD[(String, Double)] = {
    val mrRDD : RDD[(String, (Double, Int))]= rumRDD.map({x=>
      (x.getOrElse("productId","-1"),(x.getOrElse("score","0.0").toDouble,1))
    })
    // macRDD: RDD[productId,(avg,cnt)]
    val macRDD= mrRDD.reduceByKey((u1,u2)=>(u1._1+u2._1,u1._2+u2._2)).map(u=>(u._1,(u._2._1/u._2._2,u._2._2)))
    val avgTot:Double = rumRDD.map(_.getOrElse("score","0.0").toDouble).reduce(_+_)/rumRDD.count
    val min= _minVotes
    macRDD.map(u=>(u._1,(u._2._1*u._2._2  + avgTot* min)/(min+u._2._2)))
  }
}
