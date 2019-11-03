package gr.hua.glasseas

import java.util.UUID

import gr.hua.glasseas.geotools.{AISPosition, GeoPoint, Voyage}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object VoyageSparkApp {

  def main(args: Array[String]): Unit = {

    val filename = "dataset.csv"
    val shipType = "Cargo"
    val numPartitions = 8

//    System.setProperty("hadoop.home.dir","C:\\hadoop" )
    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)

    LocalDatabase.initializeDefaults()

    val gc = new GlasseasContext
    val data = gc.readData(filename,sc).filter(_.shipType.contains(shipType))
    println("Dataset converted to RDD...")
    data.keyBy(_.id).partitionBy(new BalancedPartitioner(numPartitions,data.map(_.id))).glom().flatMap{
      positions =>
        val voyages: ArrayBuffer[Voyage] = ArrayBuffer()
        positions.groupBy(_._1).foreach{
          case (id,vesselPositions) =>
            var previousPort = -1
            var voyageId = UUID.randomUUID().toString
            var voyage: ArrayBuffer[AISPosition] = ArrayBuffer()
            var previousPosition = AISPosition(-1, -1, 0.0, 0.0, 0.0, 0.0, 0.0, 0L, "", "", "", "", "")
            vesselPositions.sortBy(_._2.timestamp).map(_._2).foreach{ // loop through every position to segment the trajectory into voyages
              pos =>
                LocalDatabase.getEnclosingWaypoint(GeoPoint(pos.longitude,pos.latitude)) match {
                  case Some(port) =>
                    if (previousPort != port._2 && previousPosition.nonEmpty()) { // different port id which means that the voyage ended
                      val itinerary = s"${previousPort}_to_${port._2}"
                      voyage.append(pos)
                      val completedVoyage = geotools.Voyage(voyageId,itinerary,voyage)
                      voyages.append(completedVoyage)
                      voyage = ArrayBuffer() // begin new voyage
                      previousPort = port._2 // store the starting port of the new voyage
                      voyageId = UUID.randomUUID().toString // new distinct voyage identifier
                    }
                    else {
                      if (pos.seconds - previousPosition.seconds >= 86400 && previousPosition.nonEmpty()) { // equal to or more than a day
                        val itinerary = s"${previousPort}_to_-1"
                        val completedVoyage = geotools.Voyage(voyageId,itinerary,voyage)
                        voyages.append(completedVoyage)
                        voyage = ArrayBuffer() // begin new voyage
                        previousPort = -1 // invalid previous port
                        voyageId = UUID.randomUUID().toString // new distinct voyage identifier
                      }
                      voyage.append(pos)
                    }
                  case None =>
                    if (pos.seconds - previousPosition.seconds >= 86400 && previousPosition.nonEmpty()) { // equal to or more than a day
                      val itinerary = s"${previousPort}_to_-1"
                      val completedVoyage = geotools.Voyage(voyageId,itinerary,voyage)
                      voyages.append(completedVoyage)
                      voyage = ArrayBuffer() // begin new voyage
                      previousPort = -1 // invalid previous port
                      voyageId = UUID.randomUUID().toString // new distinct voyage identifier
                    }
                    voyage.append(pos) // no port is found, which means that the vessel is still travelling at the open sea and continues its voyage
                }
                previousPosition = pos
            }
        }
        voyages
    }.filter(v => !v.itinerary.contains("-1")).saveAsTextFile(s"$shipType-trips")



  }

}
