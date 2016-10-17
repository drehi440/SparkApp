package com.rehi.master

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.UDFRegistration
import com.databricks.spark.csv
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext


object masteringTable {
  def main(args: Array[String]): Unit = {
    println("THIS IS WORKING!")

    val conf = new SparkConf()
      .setAppName("SparkSQL").setMaster("local[4]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)//SQLContext(sc)

    import sqlContext.implicits._

    val loadFile = sqlContext.load(
      "com.databricks.spark.csv",
      Map("path" -> "partyStge.csv" /*directly give the file name, i have set file name in run as cofig argument field*/ ,
        "header" -> "true")) //csv should be , separated
    loadFile.registerTempTable("PARTYSTAGE")
    loadFile.printSchema()


    val readFile = sqlContext.table("PARTYSTAGE") //.show()
    val counterReadFile = readFile.count()
                                 
                                 
                                 println(counterReadFile)//Long

sqlContext.sql("drop table if exists output")
    println("-----------------*************************-----------------------------")

    //this is to go to the last of the table :
    def recursion1(n: Long): Unit= {

      if (n > 1) {
        recursion1(n - 1)
        readFile.withColumn("primaryKey", if($"cantactId"+ $"taxId"== lead(($"cantactId"+$"taxId").toString(),n.toInt)) $"cantactId" else lit(0)).registerTempTable("LATEST")
     /*   sqlContext.sql("select * from LATEST").show()
        println("-------------------------------------"+n+"-------------------------")
      */} else {
      
        readFile.withColumn("primaryKey",$"cantactId").saveAsTable("output")//("output.txt")//update the first Line
       /* sqlContext.sql("select * from LATEST").show()
        println("-------------------------------------"+n+"-------------------------")*/
      }
    }
    
   
    
    println("JAI HIND")
    

    recursion1(counterReadFile)
    
    
    
     
      

//    addCol.registerTempTable("MASTER")

                sqlContext.sql("select * from output").show()

  }
}
