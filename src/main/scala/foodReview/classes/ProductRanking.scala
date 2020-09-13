package foodReview.classes

import org.apache.spark.rdd.RDD

class ProductRanking {

  protected var _minVotes = 150

  /**
    * Computes a true bayesian estimate on 'dataset', to obtain the product
    * rank over the whole dataset.
    *
    * @param dataset RDD where each Map has the keys productId, userId, score.
    * @return mapped_dataset of (productId, score).
    */
  def trueBayesianEstimate(dataset: RDD[Map[String,String]]): RDD[(String, Double)] = {
    val filtered_dataset : RDD[(String, (Double, Int))]= dataset.map({ x=>
      (x.getOrElse("productId","-1"),(x.getOrElse("score","0.0").toDouble,1))
    })
    // mapped_dataset: RDD[productId,(avg,cnt)]
    //count=sum of reviews per product, avg = partial avg per product
    val mapped_dataset= filtered_dataset.reduceByKey((u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
      .map(u=>(u._1,(u._2._1/u._2._2,u._2._2)))
    //score average over the whole dataset
    val avgTot:Double = dataset.map(_.getOrElse("score","0.0").toDouble)
      .reduce(_+_)/dataset.count
    val min= _minVotes
    //map productID, bayesian avgScore
    mapped_dataset.map(u=>(u._1,(u._2._1*u._2._2  + avgTot* min)/(min+u._2._2)))
  }
}
