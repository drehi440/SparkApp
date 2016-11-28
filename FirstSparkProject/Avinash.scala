package com.rehi.master

import java.util.Scanner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.io._
import org.apache.spark.sql.functions.{unix_timestamp, to_date, from_unixtime}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveContext
import java.io.File
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.hive.HiveContext
import com.sun.prism.impl.Disposer.Target
import org.apache.spark.sql.catalyst.plans.logical.With

object Avinash {
  def main(args: Array[String]): Unit = {
      println("This is spark Running!")
      val conf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")
      val sc = new SparkContext(conf)
      val sqlContext = new HiveContext(sc)//new org.apache.spark.sql.hive.HiveContext(sc)
    
      println("This is Inside Spark!===========> sc:"+sc.getClass+" ++++||+++ SqlContext:"+sqlContext.getClass)

      import sqlContext.implicits._
    

      println("Schema Creation Status===================================================>STARTED!")
      
  
      val newList = List("DATE","MS_ID","MET_CITY_NAME","COORDINATES","ELEVATION","SUNRISE","SUNSET","UV_INDEX","Max_TemperatureC","Mean_TemperatureC","Min_TemperatureC","Dew_PointC","MeanDew_PointC","Min_DewpointC","Max_Humidity","Mean_Humidity","Min_Humidity","Max_Sea_Level_PressurehPa","Mean_Sea_Level_PressurehPa","Min_Sea_Level_PressurehPa","Max_VisibilityKm","Mean_VisibilityKm","Min_VisibilitykM","Max_Wind_SpeedKmperh","Mean_Wind_SpeedKmperh","Max_Gust_SpeedKmperh","Precipitationmm","CloudCover","Events","WindDirDegrees")
      println("Schema Creation Status===================================================>LIST OF COLUMS CREATED!")
    
      var newMap = (newList).map { _+"" }
      println("Schema Creation Status===================================================>COLUMNS MAPPED USING LIST!")
    
      var targetTable = "weather"
      println("Schema Creation Status===================================================>TARGETABLE NAME : "+targetTable)
    
      var schema = StructType(newMap.map { x => StructField(x,StringType,true ) })
      println("Schema Creation Status===================================================>SCHEMA READY TO BE USED FOR DF!")
      
      val path = "weather.csv"
      println("Schema Creation Status===================================================>PATH TO TXTFILE : "+path)
      
      val readFile = sc.textFile(path, 2).map { x => x.split(",") }.map { x => Row(x:_*) }
      println("Schema Creation Status===================================================>RDD CREATED | SPLIT COLUMN WITH '|' DELIMITOR | MAPPED AS ROW(x(1)..x(n))")
      
      var tb = sqlContext.createDataFrame(readFile,schema)
      println("Schema Creation Status===================================================>SCHEMA APPLIED TO RDD AND DF CREATED!")
      
      val database = "db2"
      sqlContext.sql("use "+database)
      println("Schema Creation Status===================================================>USE DATABASE : "+database)
      
      
      tb.select("DATE","MS_ID", "UV_INDEX","SUNRISE","SUNSET").registerTempTable(targetTable)
      println("Schema Creation Status===================================================>TABLE REGISTERED WITH NAME : "+targetTable)

      val newTable = sqlContext.table(targetTable)
//      newTable.show()
      println("Schema Creation Status===================================================>TABLE SHOW STATUS : SUCCESS!")

      println("Schema Creation Status===================================================>COMPLETED!")

      
      
      
      

      println("AGGREGATION STATUS========================================================>STARTED!")
//      groupBy("DATE").agg($"Date", max("age"), sum("expense"))
        val monthSegTable = newTable.withColumn("MONTH", month($"DATE")).show()
        
//        monthSegTable.groupBy("MS_ID","YEAR","MONTH").agg(avg("UV_INDEX").as("AVG_INDEX")).show()
  
  
  //if the query is prepared just insert into this sql statement and save it as registerTempTable
        val implementedTable = sqlContext.sql("").registerTempTable("tb1")


  }
}
