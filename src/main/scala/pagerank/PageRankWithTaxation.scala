package pagerank

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession

object PageRankWithTaxation {
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setAppName("PAGERANKIDEAL").setMaster("local[4]").set("spark.driver.host", "localhost")
    /*val spark = SparkSession
      .builder.config(sparkConf).getOrCreate()*/
    val spark = SparkSession.builder.appName("PageRankWithTaxation").getOrCreate()
    val linkSorted = spark.read
      .textFile(args(0))
      .rdd.map(x => (x.split(":")(0).toInt, x.split(":")(1)))
      .partitionBy(new HashPartitioner(100))
      .persist()
    val outlinkForEach = linkSorted.flatMapValues(y => y.trim.split(" +")).mapValues(x => x.toInt)
    val outLinkSet = outlinkForEach.groupByKey()

    var ranks = outLinkSet.mapValues(v => 1.0)


    //ranks.foreach(println)

    for (i <- 1 to 25) {
      val contribs = outLinkSet.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      // Without Taxation
      //ranks = contribs.reduceByKey(_ + _).mapValues( k => k.toInt)

      //ranks.foreach(println)

    }

    val output = ranks.collect()

    //output.foreach(println)

    //output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

    val titles = spark.read.textFile(args(1)).rdd
    //https://stackoverflow.com/questions/45502265/zipwithindex-rdd-with-initial-value
    val indexed = titles.zipWithIndex.map { case (title, index) => ((index + 1).toInt, title) }
    //indexed.foreach(println)

    val joined = ranks.join(indexed)
    //joined.foreach(println)

    val edited = joined.map { case (key, value) => (value._1, value._2) }.sortByKey(false)


    //edited.foreach(println)

    edited.saveAsTextFile(args(2))

    spark.stop()
  }
}
