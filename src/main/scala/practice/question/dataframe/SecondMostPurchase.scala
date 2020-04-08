package practice.question.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SecondMostPurchase extends Serializable {
  def readSalesFile(salesLine : String) = {
    val fields = salesLine.split("\\|");
    (fields(0).toInt, fields(1), fields(3), fields(4).replace("$", "").toDouble)
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
  }

  def readRefundLine(refundLine : String) = {
    // (refund_id, original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund_quantity)
    val fields = refundLine.split("\\|")
    (fields(0).toInt, fields(1).toInt)
  }

  def readCustomerFile(customerLine : String) = {
    val fileds = customerLine.split("\\|")
    (fileds(0), fileds(1), fileds(2))
    // (customer_id, customer_first_name, customer_last_name, phone_number )
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("NotRefundedData").master("local[*]").getOrCreate()

    import spark.implicits._

    val salesLine = spark.sparkContext.textFile("/home/hduser/Downloads/data/Sales.txt")
    val salesData = salesLine.map(readSalesFile).toDF("tr_id","customer_id", "time", "total_amount");
    val salesData_2013 = salesData.filter(salesData("time").contains("2013"))

    val refundLines = spark.sparkContext.textFile("/home/hduser/Downloads/data/Refund.txt")
    val refundData = refundLines.map(readRefundLine).toDF("refund_id", "original_tId")

    val customerLines = spark.sparkContext.textFile("/home/hduser/Downloads/data/Customer.txt")
    val customerData = customerLines.map(readCustomerFile).toDF("cus_id", "cus_first_name", "cus_last_name")

    val result = salesData_2013
      .join(refundData, salesData_2013("tr_id") === refundData("original_tId"), "left")
      .join(customerData, salesData_2013("customer_id") === customerData("cus_id"), "inner")
      .filter(refundData("original_tId") isNull)

    import org.apache.spark.sql.functions._
    val customerWisePurchase = result.groupBy("customer_id", "cus_first_name","cus_last_name")
      .agg(
        sum("total_amount").as("total_amount")
      ).orderBy(desc("total_amount")).limit(2).orderBy(asc("total_amount")).limit(1)

    customerWisePurchase
      .select("customer_id", "cus_first_name", "cus_last_name","total_amount")
      .show()
  }
}
