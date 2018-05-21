package coordinatetotaler

import org.apache.spark.sql.SparkSession


object SparkDriverV3 {

  def main(args: Array[String]): Unit = {
    
    //For locally testing in Intellij
    /*(
    val sparkConf = new SparkConf().setAppName("PAGERANKIDEAL").setMaster("local[4]").set("spark.driver.host", "localhost")
    val spark = SparkSession
      .builder.config(sparkConf).getOrCreate()
    */
    
    //Normal run operations
    val spark = SparkSession.builder.appName("SparkDriver").getOrCreate()
    
    //Reads in the kill Stats 
    val dataFrame = spark.read.option("header", "true").csv(args(0))
    //val dataFrame = spark.read.option("header", "true").csv("src/main/resources/test_d.csv")
    
    //Reads in the agg stats
    val view = spark.read.option("header","true").csv(args(1))
    //val view = spark.read.option("header","true").csv("src/main/resources/test_a.csv")
    
    //Joins the two data sets on the column "match_id"
    val joined = dataFrame.join(view, Seq("match_id"))

    //joined.show()
    //creats a <key,value> = ((map,view,x,y),1)
    var master = joined.rdd.map(s => ((s.getString(6),s.getString(12),s.getString(10).toDouble.toInt,s.getString(11).toDouble.toInt),1)).filter ( x => x._1._1 != null )
        .reduceByKey(_+_)
    
    val MiraTpp = master.filter(s => s._1._1.contains("MIRAMAR") && s._1._2.contains("tpp"))
    //val MiraFpp = master.filter(s => s._1._1.contains("MIRAMAR") && s._1._2.contains("fpp"))
    val EranTpp = master.filter(s => s._1._1.contains("ERANGEL") && s._1._2.contains("tpp"))
    //val EranFpp = master.filter(s => s._1._1.contains("ERANGEL") && s._1._2.contains("fpp"))
    
    //change to what you need for your Config
    
    //Saves each map data into one file in Folders MiraTpp and EranTpp in the fold you specify
    MiraTpp.coalesce(1,true).saveAsTextFile("File:location" + args(2) + "MiraTpp")

    EranTpp.coalesce(1,true).saveAsTextFile("File:location" + args(2) + "EranTpp")

  }
}
