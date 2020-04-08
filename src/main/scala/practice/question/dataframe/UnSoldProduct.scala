package practice.question.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object UnSoldProduct extends Serializable {

  def readSalesFile(salesLine : String) = {
    val fields = salesLine.split("\\|")
    (fields(0).toInt, fields(2).toInt)
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
  }

  def readProductFile(productLine : String) = {
    val fields = productLine.split("\\|")
    (fields(0).toInt, fields(1))
    // (product_id, product_name, product_type, product_version, product_price)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("UnSoldProduct").master("local[*]").getOrCreate()

    import spark.implicits._

    val salesLine = spark.sparkContext.textFile("/home/hduser/Downloads/data/Sales.txt")
    val salesData = salesLine.map(readSalesFile).toDF("tr_id", "product_id");

    val productsLine = spark.sparkContext.textFile("/home/hduser/Downloads/data/Product.txt")
    val productData = productsLine.map(readProductFile).toDF("pr_id", "pr_name")

    val result = productData
      .join(salesData, productData("pr_id") === salesData("product_id"), "left")
      .filter(salesData("product_id") isNull)
      .select("pr_id", "pr_name")

    result.write.mode(SaveMode.Overwrite)
      .option("header", "true").csv("/home/hduser/Downloads/data/result");
  }
}
