package udemy.lectures

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AmountSpentByCustomer extends Serializable {

  def parseLine(line : String) = {
    val data = line.split(",");
    (data(0).toInt, data(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "AmountSpentByCustomer")

    val line = sc.textFile("/home/hduser/Desktop/SparkScala/SparkScala3/customer-orders.csv")
    sc.getConf
    val spllitedLines = line.map(parseLine)
    val reducedList =  spllitedLines.reduceByKey((x,y) => x+y).map(data => (data._2, data._1))
      .sortByKey()
      .map(data => (data._2,data._1))
      .collect()

      reducedList.foreach(println)
  }
}
