package com.rehi.SparkCore
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



//key address
//key address



object tablejoinDate {
def main(args: Array[String]): Unit = {
  println("This is working")
  
  //define sparkcontext and sql Context:
   val conf = new SparkConf().setAppName("SPAPKCORE").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//   val sqlContext1 = new HiveContext(sc).setConf("hive.metastore.warehouse.dir", "C:\\Users\\drehi\\workspace\\SparkApp") 
  //include sqlContext.implicits._
   import sqlContext.implicits._
   
   //csv read and create a dataframe
  
   val table1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("Table1_weekdate.csv")
//   val table3 = sqlContext1.read.json("")

   val table2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("Table2_tprID_longDesc.csv")
   
   //table2 contains tpr_long_description which have a date to be formed as join condition :

   //create a udf to seperate a striing and take the later part to be formed as a new column data:
       val code:(String => String)=(arg:String)=>{
      arg.split(" ").last.trim() 
      
    }
    
       
    val sqlUdf = udf(code)

    
   //this dataframe have additional column as the date part itself
    val table1newCol = table1.withColumn("weekdate_date",from_unixtime(unix_timestamp(sqlUdf($"weekdate"),"mm/dd/yyyy"),"mm/dd/yyyy"))//.select($"weekdate",$"weekdate_date")//show()
    
    //we need to select tpr_id
    val table2newCol = table2.withColumn("tpr_long_description_date",from_unixtime(unix_timestamp(sqlUdf($"tpr_long_description"),"dd/mm/yy"),"mm/dd/yyyy") )//.show()
    
    
    val joinedTable = table2newCol.as('a).join(table1newCol.as('b), $"tpr_long_description_date" === $"b.weekdate_date").select($"b.category",$"b.weekdate_date", $"a.tpr_id").show()
    

  //print schema to see the columns:
//  table1.printSchema()
//  table2.printSchema()
  
  
  
  
  
  
}  
  
}