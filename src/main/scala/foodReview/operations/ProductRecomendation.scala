package foodReview.operations

import foodReview.operations.Utils._
import org.apache.spark.rdd.RDD

class ProductRecomendation extends ProductAnalyzer {

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
