package udemy.lectures

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularHero extends Serializable {

  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }

  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MostPopularHero")

    // Build up a hero ID -> name RDD
    val names = sc.textFile("/home/hduser/Desktop/SparkScala/SparkScala3/Marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)

    val lines = sc.textFile("/home/hduser/Desktop/SparkScala/SparkScala3/Marvel-graph.txt")

    val pairings = lines.map(countCoOccurences)

    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )
    val flipped = totalFriendsByCharacter.map( x => (x._2, x._1) )
    var count = 0;
    val mostPopularAsc = flipped.sortByKey().collect()
    val mostPopularDesc = flipped.sortByKey(false).collect()

    for(hero <- mostPopularAsc.take(20)) {
      val mostPopularName = namesRdd.lookup(hero._2)(0)
      println(s"$mostPopularName is the most popular superhero with ${hero._1} co-appearances.")
    }
    println("---------------------------")
    for(hero <- mostPopularDesc.take(10)) {
      val mostPopularName = namesRdd.lookup(hero._2)(0)
      println(s"$mostPopularName is the most popular superhero with ${hero._1} co-appearances.")
    }
  }
}
