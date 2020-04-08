package learn.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrame extends Serializable {

  case class Person(id:Int, name:String, age:Int, numFriends:Int)

  def mapper(line : String): Person = {
    val fields = line.split(",");
    val person : Person = Person(fields(0).toInt,fields(1),fields(2).toInt,fields(3).toInt)
    return person
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);

    val spark = SparkSession.builder().appName("FakeFriends").master("local[*]").getOrCreate()

    val lines = spark.sparkContext.textFile("/home/hduser/Desktop/SparkScala/SparkScala3/fakefriends.csv")

    val people = lines.map(mapper)

    import spark.implicits._
    val personDataSet = people.toDS().cache()

    personDataSet.printSchema()

    print("1111111111111111111111")
    personDataSet.select("name").show()

    print("222222222222222222222222")
    personDataSet.filter(personDataSet("age") < 21).show()

    print("333333333333333333333333")
    personDataSet.groupBy("age").count().show()

    print("4444444444444444444444")
    personDataSet.select(personDataSet("name"), personDataSet("name") + 10).show()

    spark.stop()
  }
}
