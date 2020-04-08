package udemy.lectures

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MaxTemperature {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
  def myMax(x:Float, y:Float) : Float = {
    var myVal: Float = 0;
    if(x>y) {
      myVal = x;
    } else {
      myVal = y;
    }
    (myVal)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MaxTemperature")

    val lines = sc.textFile("/home/hduser/Desktop/SparkScala/SparkScala3/1800.csv")

    val parsedLines = lines.map(parseLine)
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    // (x:Float, y:Float) => myMax(x,y)
    // val maxTempsByStation = stationTemps.reduceByKey((x,y) => math.max(x,y))
    // val maxTempsByStation = stationTemps.reduceByKey(math.max)
    val maxTempsByStation = stationTemps.reduceByKey((x : Float, y:Float) => myMax(x,y))
    val results = maxTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station max temperature: $formattedTemp")
    }
  }
}
