//package com.rehi.SparkCore

package com.rehi.spark.tutorial.FirstSparkProject
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object joinRdd {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SPAPKCORE").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val readFile1 = sc.textFile("Key.txt", 3).map { x => x.split(" ") }.map { x => (x(0), x(0)) }

    val readFile2 = sc.textFile("test.txt", 3).map { x => x.split(",") }.map { x => (x(0), (x(1), x(2))) }

      def sl: (Any => String) = (arg: Any) => arg.toString()
      .replaceAll("\\(", " ")
      .replaceAll("\\)", "")
      .replaceAll(" ", "")
      .trim()

    //edited by BALAMANIKANDAN T balamanikandant2016@gmail.com

    def parseString(msg: Any): String = {
      msg match {
        case tuple @ (a: Any, b: Any) => {
          a.toString() + "," + b.toString().replaceAll("[()]", "").trim()
        }
        case _ => ""
      }
    }


   //edited by BALAMANIKANDAN T balamanikandant2016@gmail.com

    //val finalTable = readFile1.join(readFile2).values.map { x => sl(x) }

   val finalTable = readFile1.join(readFile2).values.map { x => parseString(x) }

    val readFinalTable = finalTable.collect().toList.map { println }

    //println(finalTable.getClass)

  }
}
