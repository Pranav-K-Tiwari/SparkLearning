package practice.question.rdd

import scala.collection.mutable.Map
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SalesDistribution extends Serializable {

  def readProductFile(productLine : String) = {
    val fields = productLine.split("\\|");
    (fields(0), fields(1), fields(2), fields(4))
    // (product_id, product_name, product_type, product_version, product_price)
  }

  def readSalesFile(salesLine : String) = {
    val fields = salesLine.split("\\|");
    (fields(2).toInt, fields(4).replace("$", "").toInt, fields(5).toInt)
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "SalesDistribution")

    val productsLine = sc.textFile("/home/hduser/Downloads/data/Product.txt")

    val productData = productsLine.map(readProductFile)
      .map(data => (data._3,(data._1, data._2,data._4)))
      .sortByKey()
      .map(data => (data._2._1,data._2._2, data._1, data._2._3))

    val salesLine = sc.textFile("/home/hduser/Downloads/data/Sales.txt")

    val salesData = salesLine.map(readSalesFile);

    val reducedSalesDataPrice = salesData.map(x => (x._1, x._2)).reduceByKey((x,y) => x+y).collect().toMap
    val reducedSalesDataQuantity = salesData.map(x => (x._1, x._3)).reduceByKey((x,y) => x+y).collect().toMap

    val joinedData =
    for(data <- productData) {

      println(s"id=${data._1} name=${data._2} type=${data._3} Per Unit Price=${data._4} " +
        s"Quantity Sold = ${reducedSalesDataQuantity.get(data._1.toInt)}" +
        s"Total Price= ${reducedSalesDataPrice.get(data._1.toInt)}")
    }
  }

}
