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
    val filtered_dataset : RDD[(String, (Double, Int))] = Utils.time { dataset.map({ x=>
      (x.getOrElse("productId","-1"),(x.getOrElse("score","0.0").toDouble,1))
      }).cache()
    }
    // mapped_dataset: RDD[productId,(avg,cnt)]
    //count=sum of reviews per product, avg = partial avg per product

    val mapped_dataset= Utils.time { filtered_dataset.reduceByKey((u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
      .map(u=>(u._1,(u._2._1/u._2._2,u._2._2))).cache() }
    //score average over the whole dataset
    val only_score_dataset: RDD[Double] = Utils.time { dataset.map(_.getOrElse("score","0.0").toDouble)}
    val avgTot:Double = Utils.time { only_score_dataset.mean }
    //val avgTot:Double = 4.183
    println(avgTot)
    val min= _minVotes
    //map productID, bayesian avgScore
    Utils.time {
      mapped_dataset.map(u => (u._1, (u._2._1 * u._2._2 + avgTot * min) / (min + u._2._2)))
    }
  }
}
