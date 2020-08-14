package foodReview.operations

import foodReview.operations.Utils._
import org.apache.spark.rdd.RDD

class ProductRecomendation extends ProductAnalyzer {
  /**
    * Computes a df (product, score of 'user' on the product).
    *
    * @param df DataFrame with at least (userId, productId, score).
    * @param user User identifier.
    * @return DataFrame (productId, score) containing the products and scores of 'user'.
    */
  //TODO: remove
  /*private def userScores(df:DataFrame, user:String): DataFrame = {
    df.where(s"userId = '$user'").select("productId","score")
  }*/

  /**
    * Computes the products reviewed ONLY by similar users.
    *
    * @param rumDf DataFrame (userId, productId, score).
    * @param simDf DataFrame (userId,similarity) of the most similar users.
    * @param userDf DataFrame with at least (productId).
    * @return DataFrame (userId, productId, score) of the products bought by neighbors and non-bought by the requester.
    */
  //TODO: remove
  /*private def usefulNonBoughtProductScores(rumDf:DataFrame, simDf:DataFrame, userDf:DataFrame): DataFrame = {
    val df= simDf.select("userId").join(rumDf, "userId")
    df.join(userDf, df("productId") === userDf("productId"), "left_anti")

  }*/
  /**
    * Computes the products reviewed by similar users.
    *
    * @param rumDf DataFrame (userId, productId, score).
    * @param simDf DataFrame with at least (userId) of the most similar users.
    * @return DataFrame (userId, productId, score) containing the products bought by neighbors.
    */
  //TODO: remove
  /*private def usefulTotalProductsScores(rumDf:DataFrame, simDf:DataFrame): DataFrame = {
    simDf.select("userId").join(rumDf, "userId")
  }*/

  /**
    * Predicts the score of the products bought by the neighbors but non bought by the requester.
    *
    * @param rumDf DataFrame (userId, productId, score).
    * @param simDf DataFrame (userId, similarity).
    * @param userDf DataFrame (productId, score) of the requester.
    * @return DataFrame (productId, productPrediction).
    */
  //TODO: remove
  /*private def predictScore(rumDf:DataFrame, simDf:DataFrame, userDf:DataFrame): DataFrame = {
    val usefulUnbought:DataFrame = usefulNonBoughtProductScores(rumDf, simDf, userDf)
    val avgU:Double = avgScore(userDf)
    val avgDf:DataFrame = avgGroupScore(usefulTotalProductsScores(rumDf, simDf))
    val simAvgDf:DataFrame = simDf.join(avgDf, "userId")
    val sumSimilarity = simAvgDf.agg(sum("similarity")).first().get(0).toString.toDouble
    val predDf0:DataFrame = usefulUnbought.join(simAvgDf, "userId")
    val predDf1 = predDf0.withColumn("prediction", col("similarity")*(col("score") - col("avgScore")))
    val predDf2 = predDf1.groupBy("productId").agg(sum("prediction") as "productPrediction")
    predDf2.withColumn("productPrediction", (predDf2("productPrediction")/sumSimilarity) + avgU)
            .orderBy(desc("productPrediction"))
  }*/

  /**
    * Suggests products to the given user and predicts his scores.
    *
    * @param rumDf DataFrame containing at least (productId, userId, score).
    * @param user User identifier.
    * @return DataFrame (productId, productPrediction).
    * @throws InvalidOp if 'user' doesn't exist.
    */
  //TODO: remove
  /*def productRecommendation(rumDf:DataFrame, user:String): DataFrame = {
    if(!idExists(rumDf, user, "userId"))
      throw new InvalidOp("Not existing id")
    val userDf = userScores(rumDf, user)
    val comparativeDf = comparativeInfo(userDf, rumDf.where(s"not userId = '$user'"), "userId")
    val simDf = similarityNeighbors(similarityDf(rumDf, user, comparativeDf, "userId"), "userId")
    predictScore(rumDf, simDf, userDf)
  }*/

  /**
    * Suggests products to the given user and predicts his scores.
    *
    * @param rumRDD RDD where each Map has the keys (productId, userId, score).
    * @param user User identifier.
    * @return RDD (productId, productPrediction).
    * @throws InvalidOp if the user doesn't exist.
    */
  def productRecommendation(rumRDD:RDD[Map[String,String]], user:String): RDD[(String, Double)] = {
    if(!idExists(rumRDD, user, "userId"))
      throw new InvalidOp("Not existing id")
    val userRDD = rumRDD.filter(r => r.getOrElse("userId", "-1").equals(user)).map(k =>
      (k.getOrElse("productId", "-1"), k.getOrElse("score", "0.0").toDouble))
    val avgRequester = userRDD.map(_._2.toDouble).reduce((v1, v2) => v1+v2)/userRDD.count()
    val mUserSet: Set[String] = userRDD.map(_._1).collect().toSet
    val sim = similarityRDD(rumRDD, user, "userId")
    val simSum: Double = sim.values.sum
    val neighSet: Set[String] = sim.keys.toSet
    //RDD[users similar to user request]
    val neighRDD = rumRDD.filter(r => neighSet.contains(r.getOrElse("userId","-1")))
    val neighAvgs: RDD[(String, Double)] = neighRDD
      .map(r => (r.getOrElse("userId", "-1"), (r.getOrElse("score", "0.0").toDouble, 1.0)))
      .reduceByKey((m1, m2) => (m1._1.toDouble + m2._1.toDouble, m1._2.toDouble + m2._2.toDouble))
      .map(u => (u._1, u._2._1.toDouble/u._2._2.toDouble))
    //RDD[productId,first part of similarity] = sum(simUID* scoreProduct - avgUID)
    val product2Pred: RDD[(String, Double)] = neighRDD
      .filter(r => !mUserSet.contains(r.getOrElse("productId", "-1")))
      .map(r=> (r.getOrElse("userId", "-1"), (r.getOrElse("productId", "-1"), r.getOrElse("score", "0.0").toDouble)))
      .join(neighAvgs)
      .map(u => (u._2._1._1, sim.getOrElse(u._1, 0.0) * (u._2._1._2  - u._2._2)))
    // avgREQ + 1/simSum *  [first part of similarity] last part of computation of similarity
    product2Pred.reduceByKey((u1,u2) => u1 + u2).map(r => (r._1, r._2/simSum + avgRequester))
  }
}
