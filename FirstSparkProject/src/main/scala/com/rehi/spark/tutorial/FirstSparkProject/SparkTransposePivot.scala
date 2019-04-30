package com.rehi.SparkCore

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object SparkTransposePivot {
    def main(args: Array[String]): Unit = {
    println("This is SparkTransposePivot to transpose rows into Columns")
    
    val df1 = spark.sparkContext.parallelize(Seq(("A","1"),("B","2"),("C","3")),1).toDF("col1")
    
    //Assuming the first row will be the list of columns
    val new_schema = StructType(df1.select(collect_list("col1")).first().getAs[Seq[String]](0).map(z => StructField(z, StringType)))
    
    //Assuming the 2nd row would be the rows
    val new_values = sc.parallelize(Seq(Row.fromSeq(df1.select(collect_list("col2")).first().getAs[Seq[String]](0))))
    
    //creating the Dataframe using the new_schema and new_values
    spark.sqlContext.createDataFrame(new_values, new_schema).show(false)
    
    
    }
}
  
