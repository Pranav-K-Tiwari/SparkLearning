package learn.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FakeFriends extends Serializable {

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
    val personDataSet = people.toDS()

    personDataSet.printSchema();
    personDataSet.createOrReplaceTempView("person");

    val teenAgers = spark.sql("select * from person where age >=15 and age <=19").collect()

    teenAgers.foreach(println)

    spark.stop()
  }
}
