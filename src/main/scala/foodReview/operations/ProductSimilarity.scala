package foodReview.operations

import foodReview.operations.Utils._
import org.apache.spark.rdd.RDD

class ProductSimilarity extends ProductAnalyzer {

  /**
    * Computes the similarity between the given product and the products in rumDf.
    *
    * @param rumDf DataFrame containing at least (score, userId, productId).
    * @param product Product identifier.
    * @param products Product identifiers.
    * @return Sequence of DataFrames, one for each product.
    * @throws InvalidOp if one of the productIds (product + products) doesn't exist.
    */
  //TODO: remove
  /*private def similar(rumDf: DataFrame, product:String, products: String*): Seq[DataFrame] = {
    val simDfs = (product +: products).map {
      productId => {
        if (!idExists(rumDf, productId, "productId"))
          throw new InvalidOp("Not existing id")
        val productScoredf = productScore(rumDf, productId)
        val comparativeDf = comparativeInfo(productScoredf, rumDf.where(s"not productId = '$productId'"), "productId")
        similarityNeighbors(similarityDf(rumDf, productId, comparativeDf, "productId"), "productId")
      }
    }
    simDfs
  }*/

  /**
    * Computes the similarity between the given product and the products in rumRDD.
    *
    * @param rumRDD RDD where each Map has the keys (productId, userId, score).
    * @param product Product identifier.
    * @param products Product identifiers.
    * @return Sequence of (Map (productId, similarity), Map(first map productIds, 1), one for each product.
    * @throws InvalidOp if one of the productIds (product + products) doesn't exist.
    */
  private def similar(rumRDD:RDD[Map[String,String]], product:String, products: String*): Seq[(Map[String, Double], Map[String, Double])] = {
    val simRDDs = (product +: products).map {
      productId => {
        if (!idExists(rumRDD, productId, "productId"))
          throw new InvalidOp("Not existing id")
        val s = similarityRDD(rumRDD, productId, "productId")
        (s, s.map(r => (r._1, 1.0)))
      }
    }
    simRDDs
  }

  /**
    * Computes the products similar to the given one(s), by considering the others that received a similar score.
    * @note If a product is similar to only one of those given as input, its similarity is the one for that product,
    *       otherwise, if it's similar to multiple products, its similarity is the average of the ones for those products.
    *
    * @param rumDf DataFrame containing at least (score, userId, productId).
    * @param product Product identifier.
    * @param products Product identifiers.
    * @return DataFrame with schema (productId, similarity).
    */
  //TODO: remove
  /*def similarProductsUnion(rumDf: DataFrame, product:String, products: String*): DataFrame = {
    val simDfs = similar(rumDf, product, products:_*)
    val simDf:DataFrame = simDfs.reduce(_.unionByName(_))
    simDf.groupBy("productId").agg(sum("similarity")/count("similarity") as "similarity")
      .orderBy(desc("similarity"))
  }*/

  /**
    * Computes the products similar to the given one(s), by considering the others that received a similar score.
    * @note A product is considered only if it's similar to all the ones given as input, and its similarity is the average
    *       of all the results.
    *
    * @param rumDf DataFrame containing at least (score, userId, productId).
    * @param product Product identifier.
    * @param products Product identifiers.
    * @return DataFrame with schema (productId, similarity).
    */
  //TODO: remove
  /*def similarProductsIntersect(rumDf: DataFrame, product:String, products: String*): DataFrame = {
    val simDfs = similar(rumDf, product, products:_*)
    val simDf = simDfs.reduce((x,y)=> {
      val z = x.select("productId").withColumnRenamed("productId", "pid")
      val w = y.select("productId")
      val commonProducts = z.join(w, z("pid") === y("productId")).drop("productId")
      commonProducts.join(x, x("productId") === commonProducts("pid"))
        .unionByName(commonProducts.join(y, x("productId") === commonProducts("pid")))
        .drop("pid")
    })
    simDf.groupBy("productId").agg(sum("similarity")/count("similarity") as "similarity")
      .orderBy(desc("similarity"))
  }*/

  /**
    * Computes the products similar to the given one(s), by considering the others that received a similar score.
    * @note If a product is similar to only one of those given as input, its similarity is the one for that product,
    *       otherwise, if it's similar to multiple products, its similarity is the average of the ones for those products.
    *
    * @param rumRDD RDD where each Map has the keys productId, userId, score.
    * @param product Product identifier.
    * @param products Product identifiers.
    * @return Map (productId, similarity).
    */
  def similarProductsUnion(rumRDD:RDD[Map[String,String]], product:String, products: String*): Map[String, Double] = {
    val simRDDs= similar(rumRDD, product, products:_*)
    val combinedMaps = simRDDs.reduce((m1, m2) => (union(m1._1, m2._1), union(m1._2, m2._2)))
    divideMaps(combinedMaps._1, combinedMaps._2)
  }

  /**
    * Computes the products similar to the given one(s), by considering the others that received a similar score.
    * @note A product is considered only if it's similar to all the ones given as input, and its similarity is the average
    *       of all the results.
    *
    * @param rumRDD RDD where each Map has the keys productId, userId, score.
    * @param product Product identifier.
    * @param products Product identifiers.
    * @return Map (productId, similarity).
    */
  def similarProductsIntersect(rumRDD:RDD[Map[String,String]], product:String, products: String*): Map[String, Double] = {
    val simRDDs= similar(rumRDD, product, products:_*)
    val combinedMaps = simRDDs.reduce((m1, m2) => (intersect(m1._1, m2._1), intersect(m1._2, m2._2)))
    divideMaps(combinedMaps._1, combinedMaps._2)
  }
}
