package gr.hua.glasseas

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class GlasseasContext extends Serializable {

  def readStream(filename: String): Iterator[AISPosition] = scala.io.Source.fromFile(filename).getLines.drop(1).map(line => mapToPosition(line))

  def readData(filename: String, sc: SparkContext): RDD[AISPosition] = {
    val data = sc.textFile(filename)
    val header = data.first()
    data.filter(row => row != header).map(mapToPosition(_))
  }

  def readVoyageData(filename: String, sc: SparkContext): RDD[(String,ArrayBuffer[AISPosition])] = {
    val data = sc.textFile(filename)
    val header = data.first()
    data.filter(row => row != header).map{
      row =>
        val parts = row.split(",")

        val voyageId = parts.head
        val itinerary = parts(1)
        val mmsi = parts(2).toInt
        val imo = if (parts(3) == "NULL") -1 else parts(3).toInt
        val lat = BigDecimal(parts(4).toDouble).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
        val lon = BigDecimal(parts(5).toDouble).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
        val cog = BigDecimal(parts(6).toDouble).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
        val heading = if (parts(7) == "NULL") 0.0 else BigDecimal(parts(7).toDouble).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
        val speed = BigDecimal(parts(8).toDouble/10.0).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
        val seconds = convertTimestampToSeconds(parts(9))
        val timestamp = parts(9)
        val shipName = parts(10)
        val typeName = parts(11)
        val destination = parts(12)

        (itinerary,AISPosition(mmsi,imo,lat,lon,cog,heading,speed,seconds,timestamp,shipName,typeName,destination,voyageId))
    }.groupBy(_._1).map{
      case (itinerary,arr) =>
        (itinerary,arr.map(_._2).to[ArrayBuffer])
    }
  }

  private def mapToPosition(record: String): AISPosition = {
    val recordParts = record.split(",")

    val mmsi = recordParts.head.toInt
    val imo = if (recordParts(1) == "NULL") -1 else recordParts(1).toInt
    val lat = recordParts(2).toDouble
    val lon = recordParts(3).toDouble
    val cog = recordParts(4).toDouble
    val heading = if (recordParts(5) == "NULL") 0.0 else recordParts(5).toDouble
    val speed = recordParts(6).toDouble/10.0
    val seconds = convertTimestampToSeconds(recordParts(7))
    val timestamp = recordParts(7)
    val shipName = recordParts(8)
    val typeName = recordParts(9)
    val destination = recordParts(10)
    val annotation = if (recordParts.length == 12) recordParts(11) else "NULL"

    AISPosition(mmsi,imo,lat,lon,cog,heading,speed,seconds,timestamp,shipName,typeName,destination,annotation)
  }

  /**
    * Converts human timestamp to epoch seconds
    * @param timestamp
    * @return seconds
    */
  private def convertTimestampToSeconds(timestamp: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = sdf.parse(timestamp)
    dt.getTime/1000
  }

}
