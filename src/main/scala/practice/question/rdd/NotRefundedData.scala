package practice.question.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object NotRefundedData extends Serializable {

  def readSalesFile(salesLine : String) = {
    val fields = salesLine.split("\\|");
    (fields(0).toInt, fields(3), fields(4).replace("$", "").toInt)
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
  }

  def readRefundLine(refundLine : String) = {
    // (refund_id, original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund_quantity)
    val fields = refundLine.split("\\|")
    (fields(0).toInt, fields(1).toInt)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "NotRefundedData")

    val salesLine = sc.textFile("/home/hduser/Downloads/data/Sales.txt")

    val salesData = salesLine.map(readSalesFile);

    val filteredSalesData = salesData.filter(data => data._2.contains("2013")).map(data => (data._1, data._3))

    val refundLines = sc.textFile("/home/hduser/Downloads/data/Refund.txt")

    val refundData = refundLines.map(readRefundLine).collect().toMap

    val nonRefundedSales = filteredSalesData.filter(data => !refundData.contains(data._1)).collect()

    var totalSum = 0;
    nonRefundedSales.foreach(data => {
      totalSum += data._2
    })
   //  1489232
    //1637540
    print(totalSum)
  }
}
