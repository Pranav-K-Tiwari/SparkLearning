package practice.question.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SalesDistribution extends Serializable {
  case class Product(p_id:Int, name:String, p_type:String, p_price:String)
  case class Sale(product_id:Int, price:Double, quantity:Int)

  def readProductFile(productLine : String): Product = {
    val fields = productLine.split("\\|");
    val product : Product = Product(fields(0).toInt, fields(1), fields(2), fields(4))
    return product
    // (product_id, product_name, product_type, product_version, product_price)
  }

  def readSalesFile(salesLine : String): Sale = {
    val fields = salesLine.split("\\|");
    val sale : Sale = Sale(fields(2).toInt, fields(4).replace("$", "").toDouble, fields(5).toInt)
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
    return sale
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("SalesDistribution").master("local[*]").getOrCreate()

    val productsLine = spark.sparkContext.textFile("/home/hduser/Downloads/data/Product.txt")

    import spark.implicits._
    val productData = productsLine.map(readProductFile).toDS

    val salesLine = spark.sparkContext.textFile("/home/hduser/Downloads/data/Sales.txt")

    val salesData = salesLine.map(readSalesFile).toDF("product_id", "price", "quantity")

    import org.apache.spark.sql.functions._
    var productWiseTotalSales = salesData.groupBy("product_id")
      .agg(
        sum("price").as("total_price"),
        sum("quantity").as("total_quantity")
      )
      .orderBy(asc("product_id"))

    val finalResult = productWiseTotalSales.join(
      productData,
      productWiseTotalSales("product_id") ===  productData("p_id"),
      "inner"
    ).as("d")

    finalResult
      .select("d.name", "d.product_id", "d.p_price", "d.p_type", "d.total_price", "d.total_quantity")
      .show(22,false)
  }

}
