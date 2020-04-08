package practice.question.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import practice.question.dataframe.SalesDistribution.readSalesFile


object PerUserCountWithSameProductOnSameDay extends Serializable {

  def readSalesFile(salesLine : String) = {
    val fields = salesLine.split("\\|");
    val onlyDate = fields(3).split(" ")(0)
    (fields(1).toInt, fields(2).toInt, onlyDate)
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("PerUserCountWithSameProductOnSameDay").master("local[*]").getOrCreate()

    import spark.implicits._

    val salesLine = spark.sparkContext.textFile("/home/hduser/Downloads/data/Sales.txt")

    val salesData = salesLine.map(readSalesFile).toDF("cus_id", "pro_id", "timestamp")

    val groupedResult = salesData
      .groupBy("cus_id", "pro_id", "timestamp")
      .count().as("count")
      .filter($"count" >= 2)

    println(groupedResult.count())
  }
}
