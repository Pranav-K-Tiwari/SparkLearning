package udemy.lectures

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MostPopularMovie extends Serializable {
  def parsLine(line : String) = {
    val sppllited = line.split("\t");
    (sppllited(0).toInt, sppllited(1).toInt, sppllited(2).toInt, sppllited(3).toInt)
  }

  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("/home/hduser/Desktop/SparkScala/ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularMovie")

    val line = sc.textFile("/home/hduser/Desktop/SparkScala/ml-100k/u.data")
    var nameDict = sc.broadcast(loadMovieNames)
    val parsedLine = line.map(parsLine)
    val movie = parsedLine.map(data => (data._2,1)).reduceByKey((x,y) => x+y).map(data => (data._2, data._1))
    val maxx = movie.max()
    // val ll = parsedLine.filter(data => data._2 == maxx._2).collect();
    // ll.foreach(println)
    // maxx.foreach(println)
    print(nameDict.value(maxx._2),maxx._1)
  }
}
