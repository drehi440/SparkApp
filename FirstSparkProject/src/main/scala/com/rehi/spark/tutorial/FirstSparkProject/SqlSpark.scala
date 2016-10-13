package com.rehi.spark.tutorial.FirstSparkProject

//import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.UDFRegistration
import com.databricks.spark.csv
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
//import com.databricks.spark.sql
//import org.apache.spark.sql.hive.HiveContext

object SqlSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SparkSQL").setMaster("local[4]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val loadFile = sqlContext.load(
        "com.databricks.spark.csv", 
        Map("path" -> args(0)/*directly give the file name, i have set file name in run as cofig argument field*/,
            "header" -> "true")) //csv should be , separated
    loadFile.registerTempTable("CONSUMER")
    loadFile.printSchema()

    val readCONSUMER = sqlContext.sql("select * from CONSUMER") //.show()

    val changeSpace = udf { (col: String) => col.replaceAll(" ", "-") } //import org.apache.spark.sql.functions._ add this

    val newDF = readCONSUMER.withColumn("dsc_long_description_changed", changeSpace($"dsc_long_description"))
    newDF.registerTempTable("CONSUMER")
    //      sqlContext.sql("select * from LATEST")//.show()
    sqlContext.sql("select * from CONSUMER").show()
    println("----------------------------------*****************----------------------------------------")

    /*  val newReadCounsumer = readCONSUMER.map(
      row => {
        val row5 = row.getAs[String]("dsc_long_description").replaceAll(" ", "-")
        Row(row(0), row(1), row(2), row(3), row5, row(5), row(6), row(7), row(8), row(9), row(10), row(11))
      }).collect().foreach { println }*/

    //this is showing the replaced column
    /*    println(newReadCounsumer.getClass)
    println("----------------------------------*****************----------------------------------------")

    //this gives a new column with the - sign  
    val addNewColConsumer = readCONSUMER.map { x => x.getAs[String]("dsc_long_description").replaceAll(" ", "-") }.toDF()

    addNewColConsumer.registerTempTable("ConsumerNew")

    val readNewColConsumer = sqlContext.sql("select * from ConsumerNew")
    readNewColConsumer.show()
*/
    //    val readFile = sc.textFile("data.csv").map { x => x.split(",") }
    /*   .toDF("dsc_fds_id_fast_refresh","dsc_pch_id","dsc_type","dsc_external_code","dsc_long_description",
        "dsc_short_description","dsc_cou_code","dsc_oin_ord_id","dsc_oin_index","dsc_lng_code","dsc_chr_instr","dsc_level_node_char")
        
        */

    //    readFile.show()
    //    readFile.collect().foreach { println }

  }
}