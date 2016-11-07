package com.rehi.SparkCore
//http://stackoverflow.com/questions/34196302/the-root-scratch-dir-tmp-hive-on-hdfs-should-be-writable-current-permissions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.Date
//import org.apache.spark.sql.functions.unix

import org.apache.spark.sql.functions.{unix_timestamp, to_date, from_unixtime}
import org.apache.spark.sql.hive.HiveContext
//import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveContext



object HiveSpark {
  case class Record(key: Int, value: String)
  def main(args: Array[String]): Unit = {
    println("This is Hive Spark")
   
    
    val conf = new SparkConf().setAppName("SPAPKCORE").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    val hiveContext = new HiveContext(sc)
    println(hiveContext.toString())
    


                import hiveContext.implicits._

        import hiveContext.sql
    

        hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
        hiveContext.sql("LOAD DATA LOCAL INPATH '/SparkApp/kv1.txt' INTO TABLE src")

        hiveContext.sql("SELECT * FROM src").show()
    
    


/*.builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()  
*/    
    
    
  }
}