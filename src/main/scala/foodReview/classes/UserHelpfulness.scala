package foodReview.classes

import org.apache.spark.rdd.RDD


object UserHelpfulness {

  /**
    * Computes the helpfulness associated to each user.
    *
    * @note If an user has an helpfulness score that is lower than the average (of the other users that gave the same
    *       score to the same product), its score is incremented by adding the average score to the initial score and
    *       dividing by 2. The final helpfulness score is the average of the user helpfulness for the evaluated products.
    *
    * @param df RDD of Maps where each Map contains (userId, productId, score, helpfulness).
    * @return RDD (userId, userHelpfulness).
    */
  def userHelpfulness(df:RDD[Map[String,String]], threshold: Int): RDD[(String, Double)] = {
    val helpfulnessRDD = df.map(r => (
        r.getOrElse("userId", "-1"), (r.getOrElse("helpfulness", "0").toDouble, 1)))
    // RDD (userId, helpfulness)
    val userHelpRDD = helpfulnessRDD.reduceByKey((p1,p2) => (p1._1+p2._1, p1._2+p2._2)).map(r => (r._1, r._2._1/r._2._2))
    // RDD ((product, score), helpfulness)
    val avgHelpRDD = df.map(r => (
        (r.getOrElse("productId", "-1"), r.getOrElse("score", "0").toDouble),
        (r.getOrElse("helpfulness", "0").toDouble, 1)))
        .reduceByKey((mr1, mr2) => (mr1._1 + mr2._1, mr1._2 + mr2._2))
        .map(mr => (mr._1, mr._2._1/mr._2._2))
    // join on the same key
    // RDD (userId, (productId, score, avgHelpfulness))
    val jAvg = df.map(r => ((r.getOrElse("productId", "-1"), r.getOrElse("score", "0").toDouble), r.getOrElse("userId", "-1")))
        .join(avgHelpRDD)
        .map(r => (r._2._1, (r._1._1, r._1._2, r._2._2)))
    // RDD (userId, helpfulness avg dependant, 1 for cnt)
    val jUserH: RDD[(String, (Double, Int))] = jAvg.join(userHelpRDD)
        .map(r => {
          val aHelp: Double = r._2._1._3
          val uHelp: Double = r._2._2
          val fHelp: Double = if (aHelp <= uHelp) uHelp else (uHelp+aHelp)/2
          (r._1, (fHelp, 1))
        })
    val fH = jUserH.reduceByKey((c1, c2) => (c1._1 + c2._1, c1._2 + c2._2)).map(r => (r._1, r._2._1/r._2._2))
    fH.filter(k => k._2 > threshold)
  }
}
