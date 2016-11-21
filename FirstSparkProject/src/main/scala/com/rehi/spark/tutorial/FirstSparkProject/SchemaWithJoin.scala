package com.rehi.SparkCore
//Code is generic to create Schema and join two tables!



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.io._
//import org.apache.spark.sql.functions.unix

import org.apache.spark.sql.functions.{unix_timestamp, to_date, from_unixtime}
import org.apache.spark.sql.hive.HiveContext
//import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveContext
import java.io.File
import scala.collection.mutable.ListBuffer

object SchemaWithJoin {
  var tableCol = ListBuffer[Any]()
  def main(args: Array[String]): Unit = {
    println("THIS IS WORKING")

  val conf = new SparkConf().setAppName("SPAPKCORE").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
     val hiveContext = new HiveContext(sc)

      import hiveContext.implicits._
      import hiveContext.sql
    
      //Drop Database if exists:
      hiveContext.sql("DROP DATABASE IF EXISTS db1 CASCADE")
      hiveContext.sql("DROP DATABASE IF EXISTS db2 CASCADE")
      
      
  //create two seperate Databases which we will use to create tables and used them for different Schema builder:
      hiveContext.sql("CREATE DATABASE IF NOT EXISTS db1")
      hiveContext.sql("CREATE TABLE IF NOT EXISTS db1.Employee (row STRING, name STRING, location STRING, company STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE")
      hiveContext.sql("LOAD DATA LOCAL INPATH 'C:/Users/drehi/workspace/SparkApp/Employee.txt' INTO TABLE db1.Employee")
      
      
      //for db2:for local there could be only one DB so will use DERBY
      hiveContext.sql("CREATE DATABASE IF NOT EXISTS db2")
      hiveContext.sql("CREATE TABLE IF NOT EXISTS db2.EmpCompany (compId STRING, name STRING, company STRING, technology STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE")
      hiveContext.sql("LOAD DATA LOCAL INPATH 'C:/Users/drehi/workspace/SparkApp/EmpCompany.txt' INTO TABLE db2.EmpCompany")
      

      hiveContext.sql("select * from db1.Employee").show()
      hiveContext.sql("select * from db2.EmpCompany").show()
  
  //==================================================================================================================================
      //take columns from each table and store in list:
      def takeCol(table:Any):ListBuffer[Any]={
     
        var readCol=hiveContext.sql("desc "+table)
        println("These are the Columns to be used for Schema:")
        var newColList = readCol.map { x=> x(0)}.collect().toList
        
        for(i<- newColList){
          println("Column present in table:"+table+"==========>"+i)
        tableCol += i  
        }
        
        tableCol
        
    }
      
    
    
    
    
    //method to map the Rdd on the basis of column count:
    //==============================================================================================
    //create Schema:
    def createSchema(listCol: ListBuffer[Any], path: String,targetTable:String):Unit={
      println("the Path to be Read as:"+path)
      println("")
      println("The Schema to be created for the first Col:")
      
      
      
      var alphabet = (listCol.toList).map(_ +"")
      var schema = StructType(alphabet.map { x => StructField(x,StringType,true ) })
      println("Schema Created")
      
      //Count the number of mappings:
      var   count = listCol.size
      println(count)
      
     
      var rdd1 = sc.textFile(path,2).map { x => x.split(",") }.map { x => Row(x:_*) }
      println("Sucessfull mapped rdd to Row Schema")
      println("Initiate apply Schema")
      var tb = hiveContext.createDataFrame(rdd1,schema)
      println("Successfully Applied Schema!")
      
      var finalTable1 =tb.coalesce(1).write
      
      finalTable1.mode(SaveMode.Overwrite).saveAsTable(targetTable)
//      tb.coalesce(1).write().mode(SaveMode.Overwrite).save("dbName.Demo121_bd"
      
    }
    
    //==============================================================================================
    //get the TableColumns for rdd1:
    takeCol("db1.Employee")
    tableCol.map { x => println(x) }
    println("total Elements : "+tableCol.size)
    println("Col's are Stored in List tableCol: SUCCESS")
    
    //given Paths of the tables to be read as Rdd
    val path1="C:\\user\\hive\\warehouse\\db1.db\\employee\\Employee.txt"
    val targetName = "db1.MYRDDEMPLOYEE"
    
    //Call createSchema:
    println("Passing :==============>"+tableCol.toString()+ "|| Path:======================>"+path1+"|| targetName:==========================>"+targetName)
    createSchema(tableCol, path1,targetName)
    
      hiveContext.sql("Select * from "+targetName).show()
      val table1New = hiveContext.sql("Select * from "+targetName)
      println("END OF SCHEMA CREATION!")
      
      //Empty tableCol
      tableCol.clear()
  //==================================================================================================================================
      //location of table1 and table2 will be used to create rdds :
    
       
       //get the TableColumns for rdd2:
    takeCol("db2.EmpCompany")
    tableCol.map { x => println(x) }
    println("total Elements : "+tableCol.size)
    println("Col's are Stored in List tableCol: SUCCESS")
    
    
    //given Paths of the tables to be read as Rdd
    val path2="C:\\user\\hive\\warehouse\\db2.db\\empcompany\\EmpCompany.txt"
    val targetName1 = "db2.MYRDDEMPCOMPANY"
    
    
    
     //Call createSchema:
    println("Passing :==============>"+tableCol.toString()+ "|| Path:======================>"+path2+"|| targetName:==========================>"+targetName1)
    createSchema(tableCol, path2,targetName1)
      
      hiveContext.sql("Select * from "+targetName1).show()
      val table2New = hiveContext.sql("Select * from "+targetName1)
       println("END OF SCHEMA CREATION!")
   
       
       //Empty tableCol
       tableCol.clear()
  //==================================================================================================================================
      //join 2 tables and show the results:
       
       
       val joinedTable = table1New.as('a).join(table2New.as('b), $"a.company"===$"b.company").show()
    
      
 
  
  println("FINISH")
  }
  
}
