package foodReview

import java.io.File

import foodReview.operations._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
// import java.io.FileWriter // Import required to keep track of elapsed time


object Main{

  def time[R](block: => R): Double = {
    val t0 = System.nanoTime()
    val _ = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + "s")
    (t1 - t0)/1000000000.0
  }

  def main(args: Array[String]): Unit = {
    val execution = sys.env.getOrElse("environment", "aws")
    val path = sys.env.getOrElse("path", "")
    val appName = "FoodReview"

    val spark:SparkSession = execution match {
      case "aws" => org.apache.spark.sql.SparkSession.builder
                              .appName(appName)
                              .config("spark.sql.broadcastTimeout", "360000")
                              .getOrCreate
      case "local" => org.apache.spark.sql.SparkSession.builder
                              .master("local[*]")
                              .appName(appName)
                              .config("spark.sql.broadcastTimeout", "360000")
                              .getOrCreate
    }
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val intro = "---------------------------------------------------------\n" +
                "---------------------- Food Review ----------------------\n" +
                "---------------------------------------------------------\n" +
                "---------------------- Version 1.0 ----------------------\n" +
                "---------------------------------------------------------\n\n"
    println(intro)


    val resourcesFile = path + "resources/"
    val pathDatabase :String = Utils.downloadAndSaveDataset(spark, path=resourcesFile)
    val testDir = path + "src/test/scala/"
    val filename = testDir + sys.env.getOrElse("testFileName", "totalTest.txt")

    println(s"Testing filename = $filename ")

    val rk = new ProductRanking()
    val rc = new ProductRecomendation()

    println("Loading dataset ...\n")
    //import Dataset from txt
    var rumRDD: RDD[Map[String, String]] = Dataset.importRDD(spark, pathDatabase)
    println(s"Original dataset loaded with ${rumRDD.count} reviews")
    val originalDatasetSize = new File(pathDatabase).length()
    println(s"Dataset is ~ ${Utils.transformBytes(originalDatasetSize)}")


    val size = sys.env.getOrElse("size", 1).toString.toDouble
    if (size != 1.0){
      rumRDD = {
        println(s"Changing size of database by $size times. Dataset will be ~${Utils.transformBytes((originalDatasetSize * size).toLong)}")
        val enlargedRDD: RDD[Map[String, String]] = Dataset.increaseSizeDf(spark.sparkContext, rumRDD, size = size)
        println(s"Dataset now has ${enlargedRDD.count} of reviews")
        enlargedRDD
      }
    }

    //throw new InvalidOp("exit")

    var elapsedTime = 0.0
    val limitResult = 20
    val fileSource = Utils.openFile(spark, filename)
    for (line <- fileSource.rdd.collect) {
      val tmpOp = line
      try {
        val op = tmpOp.trim.split("\\s+")
        var result = spark.emptyDataFrame
        op(0) match {
          case "recommend" =>
            print("Computing Product Recommendation for ")
            var user: String = ""
            user = if (op.length < 2) {
              //Pick random userID
              print("random userID = ")
              rumRDD.takeSample(withReplacement = false, 1)(0).getOrElse("userId",
                throw new InvalidOp("userId not found"))
            } else {
              print("chosen userID = ")
              op(1)
            }
            println(s"$user")
            elapsedTime = time {
              val r = rc.productRecommendation(rumRDD, user)
                .toDF("productId", "productPrediction")
                .orderBy($"productPrediction".desc, $"productId")
                .limit(limitResult)
              r.show(limitResult)
              result = r
            }
            val fileName = s"RECOMMEND__$user" +  ".csv"
            Dataset.storeDfPt1(result, resourcesFile + fileName)
            println("Results saved in " + resourcesFile+fileName)
          case "rank" =>
            println("Computing Product Ranking ...")
            elapsedTime = time {
              val r = rk.trueBayesianEstimate(rumRDD)
                .toDF("productId", "score")
                .orderBy($"score".desc, $"productId")
                .limit(limitResult)
              r.show(limitResult)
              result = r
            }
            val fileName = "RANK.csv"
            Dataset.storeDfPt1(result, resourcesFile + fileName)
            println("Results saved in " + resourcesFile+fileName)
          case "evolutionM" =>
            if (op.length < 3)
              throw new InvalidOp("Not allowed operation")
            val year = if (op(1).forall(_.isDigit)) op(1) else null
            if (year == null)
              throw new InvalidOp("Valid year not provided")
            val product = op(2)
            val products: Seq[String] = op.slice(3, op.length)
            println(s"Computing monthly time analysis for the year=$year and productsID=$product ${products.mkString(" ")}")
            try{
              elapsedTime = time {
                    result = ProductTimeAnalysis.timeDFR(spark, rumRDD, product, products: _*)(byMonth = true, yBegin = year).orderBy("month")
                    result.show()
              }
              val fileName = s"PM__$product" + s"_${products.mkString("_")}"+s"_YEAR_$year" + ".csv"
              Dataset.storeDfPt1(result, resourcesFile + fileName)
              println("Results saved in " + resourcesFile+fileName)
            } catch {
              case _: UnsupportedOperationException => println("Not existing year in the dataset")
            }
          case "evolutionY" =>
            if (op.length < 4)
              throw new InvalidOp("Not allowed operation")
            var yb = if (op(1).forall(_.isDigit)) op(1) else null
            var ye = if (op(2).forall(_.isDigit)) op(2) else null
            val product = op(3)
            val products: Seq[String] = op.slice(4, op.length)

            println(s"Computing yearly time analysis for productsID=$product ${products.mkString(" ")} between the years $yb-$ye")
            elapsedTime = time {
                result = ProductTimeAnalysis.timeDFR(spark, rumRDD, product, products: _*)(byMonth = false, yBegin = yb, yEnd = ye).orderBy("year")
                result.show()
                if (yb == null) {
                  yb = result.select("year").first.getInt(0).toString
                  println(s"Year of beginning for productID=$product is ${products.mkString(" ")} $yb")
                }
                if (ye == null) {
                  ye = result.orderBy($"year".desc).select("year").first.getInt(0).toString
                  println(s"Year of end for productID=$product is ${products.mkString(" ")} $ye")
                }
            }
            //val fileName = "PY" + LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss")) + ".csv"
            val fileName = s"PY_$product" + s"_${products.mkString("_")}"+s"_YEARS_$yb"+s"_$ye" + ".csv"
            Dataset.storeDfPt1(result, resourcesFile + fileName)
            println("Results saved in " + resourcesFile+fileName)
          case "helpfulness" =>
            val threshold : Int = op.lift(2).getOrElse("0").toInt
            val limit : Int = op.lift(3).getOrElse("20").toInt
            println(s"Computing User Helpfulness with threshold=$threshold and limiting to $limit results...")
            elapsedTime = time {
              val r = UserHelpfulness.userHelpfulness(rumRDD, threshold).toDF(
                "userId", "userHelpfulness")
                .orderBy(-$"userHelpfulness", $"userId")
                .limit(limit)
              r.show(limit)
              result = r
            }
            val fileName = s"HELPFULNESS_threshold_$threshold" + s"_limit_$limit"+ ".csv"
            Dataset.storeDfPt1(result, resourcesFile + fileName)
            println("Results saved in " + resourcesFile+fileName)
          case "" =>
          case _ => throw new InvalidOp("Not allowed operation")
        }
      } catch {
        case e: InvalidOp => println(e.getMessage)
      }
    }
    println("\n\n"+intro)
    println("Have you found this tool useful?\n" +
      "Let us know, send us an e-mail to gabriele.calarota@studio.unibo.it or alberto.drusiani@studio.unibo.it")
    spark.sparkContext.stop()
  }
}
