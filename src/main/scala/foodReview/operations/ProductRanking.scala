package foodReview.operations

import org.apache.spark.rdd.RDD

class ProductRanking {

  protected var _minVotes = 150

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
