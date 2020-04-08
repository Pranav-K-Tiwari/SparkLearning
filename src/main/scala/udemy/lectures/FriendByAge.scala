package udemy.lectures



import org.apache.log4j._
import org.apache.spark._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge extends Serializable {

  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
    val fields = line.split(",")
    val name = fields(1).toString
    val numFriends = fields(3).toInt
    (name, numFriends)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "udemy.lectures.FriendsByAge")

    val lines = sc.textFile("/home/hduser/Desktop/SparkScala/SparkScala3/fakefriends.csv")

    val rdd = lines.map(parseLine)

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    val results = averagesByAge.collect()

    results.sorted.foreach(println)
  }

}
  