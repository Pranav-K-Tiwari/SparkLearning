package udemy.lectures

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount");

    val fileContent  = sc.textFile("/home/hduser/Desktop/SparkScala/SparkScala3/book.txt")

    val words = fileContent.flatMap(x => x.split("\\W+"));
    val lowercaseWords = words.map(x => x.toLowerCase)
    val reducedWords = lowercaseWords.map(x => (x,1)).reduceByKey((x,y) => x+y)
    val countPerWord = reducedWords.map(x => (x._2, x._1)).sortByKey().collect();
    for (result <- countPerWord) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}
