package foodReview.operations

import org.apache.spark.rdd.RDD


class ProductAnalyzer {

  protected var _minCommonProducts = 1
  protected var _minCommonUsers = 1
  protected var _neighbors = 1000

  //TODO: REMOVE
  //protected var _simMethod: SimilarityMethod.Value = SimilarityMethod.COSINE

  //TODO: REMOVE
  /*object SimilarityMethod extends Enumeration {
    type SimilarityMethod = Value
    val COSINE, PEARSON = Value
  }*/

  /*def minCommonProducts: Int= _minCommonProducts
  def minCommonProducts_=(m:Int):Unit = _minCommonProducts = m

  def minCommonUsers: Int = _minCommonUsers
  def minCommonUsers_=(u:Int):Unit = _minCommonUsers = u

  def neighbors: Int = _neighbors
  def neighbors_=(n:Int):Unit = _neighbors = n*/

  //TODO: REMOVE
  /*
  def simMethod: SimilarityMethod.Value = _simMethod
  def simMethod_=(m:String):Unit = {
    m match {
      case "COSINE" => _simMethod = SimilarityMethod.COSINE
      case "PEARSON" => _simMethod = SimilarityMethod.PEARSON
    }
  }*/
  //until here

  /**
    * Computes a df containing info on common 'usefulCol' ('usefulCol', score of the requester, score for 'usefulCol').
    *
    * @param requiringDf DataFrame ('other' (complementary of 'usefulCol'), score) of the requester.
    * @param othersDf DataFrame ('usefulCol', 'other', score) of the ones with at least an element in common with
    *                 the requester.
    * @return DataFrame with schema ('usefulCol', score, score1).
    */
  //TODO: remove
  /*protected def comparativeInfo(requiringDf: DataFrame, othersDf: DataFrame, usefulCol: String): DataFrame = {
    val (otherCol, minCommon) = usefulCol match {
      case "productId" => ("userId", _minCommonUsers)
      case "userId" => ("productId", _minCommonProducts)
    }
    val r = othersDf.withColumnRenamed("score", "score1")
    val r2 = requiringDf.join(r, otherCol).select(usefulCol, "score", "score1")
    val r3 = r2.select(usefulCol).groupBy(usefulCol).agg(count(usefulCol) as "count")
      .where(s"count >= '$minCommon'")
    val r4 = r3.select(usefulCol).withColumnRenamed(usefulCol, "usc")
    r4.join(r2, r2(usefulCol) === r4("usc")).drop("usc")
  }*/

  /**
    * Computes the similarity score with 'requester'.
    *
    * @param rumDf DataFrame with schema (score, userId, productId).
    * @param requester User or productId identifier representing the element with whom to compute the similarity.
    * @param comparativeDf DataFrame with schema ('columnId', score, score1).
    * @param column Column of the similarity aggregate.
    * @return DataFrame with schema ('columnId', similarity).
    */
  //TODO: remove
  /*protected def similarityDf(rumDf: DataFrame, requester: String, comparativeDf: DataFrame, column: String): DataFrame = {
    val (distinctIds, group, cnt): (DataFrame, String, String) = column match {
      case "productId" => (distinctProducts(comparativeDf), "productId", "userId") //ProductSimilarity
      case "userId" => (distinctUsers(comparativeDf), "userId", "productId") //ProductRecommendation
    }
    val avgs: (Double, DataFrame) = _simMethod match {
      case SimilarityMethod.COSINE =>
        (0.0, distinctIds.withColumn("avgScore", lit(0.0)))
      case SimilarityMethod.PEARSON =>
        val avgDf = avgGroupScore(rumDf, group, cnt)
        val avgUser = avgDf.where(s"$group = '$requester'").first().get(1).toString.toDouble
        (avgUser, avgDf)
    }
    val rnDf = comparativeDf.withColumnRenamed(group, "grp")
    val totDf = rnDf.join(avgs._2, avgs._2(group) === rnDf("grp")).drop("grp")
    val simFactors = totDf.select(
      col(column),
      callUDF("square", col("score"), lit(avgs._1)) as "sq",
      callUDF("square", col("score1"), col("avgScore")) as "sq1",
      callUDF("product", col("score"), col("score1"), lit(avgs._1), col("avgScore")) as "prod"
    )
    val simDf = simFactors.groupBy(column).agg(
      sum("prod") as "N",
      sum("sq") as "D1",
      sum("sq1") as "D2"
    )
    simDf.withColumn("similarity", col("N") / sqrt(col("D1") * col("D2")))
      .select(column, "similarity")
  }*/

  /**
    * Computes the df of the 'nb' neighbors with the highest similarity.
    *
    * @param simDf DataFrame with at least(similarity).
    * @param nb Number of required neighbors.
    * @return DataFrame with the same schema of 'simDf' limited to 'nb' users.
    */
  //TODO: remove
  /*protected def similarityNeighbors(simDf: DataFrame, colName:String, nb: Int = _neighbors): DataFrame = {
    simDf.orderBy(desc("similarity"), desc(colName)).limit(nb)
  }*/

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
}
