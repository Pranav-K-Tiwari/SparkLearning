package practice.question.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object NotRefundedData extends Serializable {

  def readSalesFile(salesLine : String) = {
    val fields = salesLine.split("\\|");
    (fields(0).toInt, fields(3), fields(4).replace("$", "").toDouble)
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
  }

  def readRefundLine(refundLine : String) = {
    // (refund_id, original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund_quantity)
    val fields = refundLine.split("\\|")
    (fields(0).toInt, fields(1).toInt)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("NotRefundedData").master("local[*]").getOrCreate()

    val salesLine = spark.sparkContext.textFile("/home/hduser/Downloads/data/Sales.txt")

    import spark.implicits._
    val salesData = salesLine.map(readSalesFile).toDF("tr_id", "time", "total_amount");

    val salesData_2013 = salesData.filter(salesData("time").contains("2013"))

    val refundLines = spark.sparkContext.textFile("/home/hduser/Downloads/data/Refund.txt")

    val refundData = refundLines.map(readRefundLine).toDF("refund_id", "original_tId")

    val result = salesData_2013.join(
      refundData,
      salesData_2013("tr_id") === refundData("original_tId"),
      "left"
    ).filter(refundData("original_tId") isNull)

    import org.apache.spark.sql.functions._
    result.agg(sum("total_amount")).show()
  }
}
