package gr.hua.glasseas

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import gr.hua.glasseas.geotools.{AISPosition, GeoPoint, SpatialToolkit, Voyage}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object VoyageSparkApp {

  def main(args: Array[String]): Unit = {

    val filename = "dataset.csv"
    val shipType = "Tanker"
    val numPartitions = 8

    LocalDatabase.initialize(s"waypoints/dataset_${shipType}_waypoints_2000.0_10.csv")

    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val gc = new GlasseasContext
    val data = gc.readData(filename,sc).filter(_.shipType.contains(shipType))
    data.keyBy(_.id).partitionBy(new BalancedPartitioner(numPartitions,data.map(_.id))).glom().flatMap{
      positions =>
        val voyages: ArrayBuffer[Voyage] = ArrayBuffer()
        positions.groupBy(_._1).foreach{
          case (id,vesselPositions) =>
            var previousPort = -1
            var voyageId = UUID.randomUUID().toString
            var voyage: ArrayBuffer[AISPosition] = ArrayBuffer()
            val positions = vesselPositions.sortBy(_._2.timestamp)
            var previousPosition = positions.head._2

            def splitVoyage(duration: Int, pos: AISPosition): Unit = {
              if (pos.seconds - previousPosition.seconds >= duration) {
                beginNewVoyage(-1)
              }
            }

            def beginNewVoyage(newWaypoint: Int): Unit = {
              val itinerary = s"${previousPort}_to_$newWaypoint"
              // remove positions located inside waypoints
              val completedVoyage = Voyage(voyageId,itinerary,voyage.filter(p => LocalDatabase.getEnclosingWaypoint(GeoPoint(p.longitude,p.latitude)).isEmpty))
              voyages.append(completedVoyage)
              voyage = ArrayBuffer() // begin new voyage
              previousPort = newWaypoint
              voyageId = UUID.randomUUID().toString // new distinct voyage identifier
            }

            positions.foreach{ // loop through every position to segment the trajectory into voyages
              case (mmsi,pos) =>
                LocalDatabase.getEnclosingWaypoint(GeoPoint(pos.longitude,pos.latitude)) match {
                  case Some(port) =>
                    if (previousPort != port._2) { // different port id which means that the voyage ended
                      voyage.append(pos)
                      beginNewVoyage(port._2)
                    }
                    else {
                      splitVoyage(86400,pos) // equal to or more than a day
                      voyage.append(pos)
                    }
                  case None =>
                    splitVoyage(86400,pos) // equal to or more than a day
                    voyage.append(pos) // no port is found, which means that the vessel is still travelling at the open sea and continues its voyage
                }
                previousPosition = pos
            }
        }
        voyages
    }.filter(v => !v.itinerary.contains("-1") && v.voyagePositions.size >= 3).saveAsTextFile(s"$filename-$shipType-voyages")
  }

}
