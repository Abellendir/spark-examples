
package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner


object SparkCleaner {

  /**
    * java public void main Unit returns void
    * @param args
    */
  def main(args: Array[String]): Unit ={

        val spark = SparkSession.builder.appName("SparkDriver").getOrCreate()
       
        val data = spark.read.textFile(args(0)).rdd.map{            
            s => (s.replace('(', ' ').replace(')', ' ').split(","))
        }
        //data.foreach()
        val formatted = data.map(s => (s.mkString(",").trim))
        //formatted.foreach(println)
        
        formatted.coalesce(1,true).saveAsTextFile("file:location" + args(1) + "EranTppCleaned")

      
      
    }//End Main
  
}//End Class
