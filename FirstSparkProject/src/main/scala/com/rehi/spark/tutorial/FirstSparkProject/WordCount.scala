package com.rehi.spark.tutorial.FirstSparkProject

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
   def main(args: Array[String]): Unit = {
    
     //add the project configuration into the conf:
    val conf = new SparkConf()
    .setAppName("SparkWordCount").setMaster("local[4]")
    
    //pass the configuration conf to the sparkContext sc:
    val sc = new SparkContext(conf)
    
    //read the file from a specific folder and give a min partition:
    val readFile = sc.textFile("words.txt", 4)
    
    
    //count the word by spliting line with " " and giving each word a count 1 and then reducing them by Key
    val countWord = readFile.flatMap { 
      lines => lines.split(" ")
      }
    .map { 
      words => (words,1)
      }
    .reduceByKey(_ + _)
    
    
    //this is to print the count of each word in the console 
    val showCount = countWord.collect().toList
    
    showCount.map(x=> println(x))
    
    //this is to save the count to an specific directory
    //val saveCount = countWord.saveAsTextFile("SparkCount")//this will be saved as a directory with _SUCCESS and part files
    
    
  }
  
}