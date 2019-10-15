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

  def readVoyageData(filename: String, sc: SparkContext): RDD[(String,ArrayBuffer[(String,AISPosition)])] = {
    val data = sc.textFile(filename)
    val header = data.first()
    data.filter(row => row != header).map{
      row =>
        val parts = row.split(",")

        val voyageId = parts.head
        val itinerary = parts(1)
        val mmsi = parts(2).toInt
        val imo = if (parts(3) == "NULL") -1 else parts(3).toInt
        val lat = parts(4).toDouble
        val lon = parts(5).toDouble
        val cog = parts(6).toDouble
        val heading = if (parts(7) == "NULL") 0.0 else parts(7).toDouble
        val speed = parts(8).toDouble/10.0
        val seconds = convertTimestampToSeconds(parts(9))
        val timestamp = parts(9)
        val shipName = parts(10)
        val typeName = parts(11)
        val destination = parts(12)

        (voyageId,itinerary,AISPosition(mmsi,imo,lat,lon,cog,heading,speed,seconds,timestamp,shipName,typeName,destination))
    }.groupBy(_._2).map(x => (x._1,x._2.map(y => (y._1,y._3).to[ArrayBuffer])))
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

    AISPosition(mmsi,imo,lat,lon,cog,heading,speed,seconds,timestamp,shipName,typeName,destination)
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
