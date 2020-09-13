package foodReview.classes

import foodReview.classes.Utils.idExists
import org.apache.spark.rdd.RDD

class ProductRecommendation {

  protected var _minCommonProducts = 1
  protected var _minCommonUsers = 1
  protected var _neighbors = 1000

  /**
    * Computes the similarity between 'requester' and the ids in 'usefulCol' of 'rumRDD'.
    *
    * @param rumRDD RDD containing userId, productId, score.
    * @param requester Identifier of the requester.
    * @param usefulCol Column required in the final comparison.
    * @return Map '_neighbors' elements of 'usefulCol' -> similarity.
    */
  protected def similarityRDD(rumRDD: RDD[Map[String, String]], requester: String, usefulCol: String): Map[String, Double] = {
    val (otherCol, minCommon) = usefulCol match {
      case "productId" => ("userId", _minCommonUsers)
      case "userId" => ("productId", _minCommonProducts)
    }
    val reqScoreRDD = rumRDD.filter(r => r.getOrElse(usefulCol, "-1").equals(requester))
      .map(k => (k.getOrElse(otherCol, "-1"), k.getOrElse("score", "0.0").toDouble))
    val relatedSet = reqScoreRDD.map(_._1).collect().toSet
    // related info excluding the requester
    val relatedInfoRDD = rumRDD.filter(r => !r.getOrElse(usefulCol, "-1").equals(requester) &&
      relatedSet.contains(r.getOrElse(otherCol, "-1")))
      .map(k => (k.getOrElse(usefulCol, "-1"), (k.getOrElse(otherCol, "-1"), k.getOrElse("score", "0.0").toDouble)))
    val toCompare = relatedInfoRDD.map(r => (r._1, 1)).reduceByKey((r1, r2) => r1 + r2)
      .filter(_._2 >= minCommon)
      .map(_._1)
      .collect().toSet
    val comparativeRDD = relatedInfoRDD.filter(r => toCompare.contains(r._1))
      .map(r => (r._2._1, (r._1, r._2._2))).join(reqScoreRDD)
      .map(r => (r._2._1._1, (math.pow(r._2._2.toDouble, 2), math.pow(r._2._1._2, 2), r._2._2 * r._2._1._2)))
    val simFactors = comparativeRDD.reduceByKey((u1, u2) =>
      (u1._1 + u2._1, u1._2 + u2._2, u1._3 + u2._3))
    val sim : Map[String, Double] = simFactors.map(u => (u._1, u._2._3 / math.sqrt(u._2._1 * u._2._2)))
      .sortBy(r => (r._2, r._1), numPartitions = 1, ascending = false)
      .take(_neighbors).toMap
    sim
  }

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
