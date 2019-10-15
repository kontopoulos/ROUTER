package gr.hua.glasseas.geotools

import java.io.FileWriter
import java.util.UUID

import gr.hua.glasseas.ml.clustering.DBScan
import gr.hua.glasseas.{AISPosition, BalancedPartitioner, GlasseasContext, Global, Voyage}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

case class ClusterStatistics(startLon: Double, startLat: Double, endLon: Double, endLat: Double, avgCog: Double, avgSpeed: Double, speedDeviation: Double, cogDeviation: Double) {
  override def toString: String = s"$startLon,$startLat,$endLon,$endLat,$avgCog,$avgSpeed,$speedDeviation,$cogDeviation"
}

class Preprocessor extends Serializable {

  def extractRoutes(filename: String, shipType: String, sc: SparkContext, numPartitions: Int, saveToFile: Boolean): RDD[Voyage] = {
    val gc = new GlasseasContext
    //val data = sc.parallelize(gc.readStream(filename).filter(x => x.shipType.contains("Container")).toSeq)
    val data = gc.readData(filename,sc).filter(_.shipType.contains(shipType))
    println("Dataset converted to RDD...")
    val vesselVoyages = data.keyBy(_.mmsi).partitionBy(new BalancedPartitioner(numPartitions,data.map(_.mmsi))).glom().flatMap{
      positions =>
//        var voyages: Map[String,Voyage] = Map()
        val voyages: ArrayBuffer[Voyage] = ArrayBuffer()
        //var tripsPerVoyage: Map[String,Int] = Map()
        positions.groupBy(_._1).foreach{
          case (id,vesselPositions) =>
            var previousPort = -1
            var voyageId = UUID.randomUUID().toString
            var voyage: ArrayBuffer[AISPosition] = ArrayBuffer()
            vesselPositions.sortBy(_._2.timestamp).foreach{ // loop through every position to segment the trajectory into voyages
              case (mmsi,pos) =>
                Global._ports.find(x => x._1.isInside(GeoPoint(pos.longitude,pos.latitude))) match {
                  case Some(port) =>
                    if (previousPort != port._2) { // different port id which means that the voyage ended
                      val itinerary = s"${previousPort}_to_${port._2}"
                      val completedVoyage = Voyage(voyageId,itinerary,voyage)
                      voyages.append(completedVoyage)
//                      voyages.get(voyageId) match {
//                        case Some(v) =>
//                          val newVoyageList = v ++ voyage
//                          voyages += (voyageId -> newVoyageList)
//                        case None =>
//                          voyages += (voyageId -> voyage)
//                      }
                      voyage = ArrayBuffer() // begin new voyage
                      previousPort = port._2 // store the starting port of the new voyage
                      voyageId = UUID.randomUUID().toString // new distinct voyage identifier
                    }
                    else voyage.append(pos)
                  case None =>
                    voyage.append(pos) // no port is found, which means that the vessel is still travelling at the open sea and continues its voyage
                }
            }
        }
        /*val filtered = tripsPerVoyage.filter(_._2 > 1).keySet
        voyages.filter(v => filtered.contains(v._1))*/
        voyages
    }//.filter(x => !x._1.contains("(-1)"))

    if (saveToFile) {
      println("Saving results to file...")
      val w = new FileWriter(filename.replace(".csv","") + "_" + shipType +"_voyages.csv")
      w.write("VOYAGE_ID,ITINERARY,MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SOG,TIMESTAMP,NAME,SHIP_TYPE,DESTINATION\n")
      vesselVoyages.collect().foreach {
        voyage =>
          w.write(s"$voyage\n")
//          positions.foreach(p => w.write(s"$voyageId,${p.toString}\n"))
      }
      w.close()
      println("Save complete.")
    }
    vesselVoyages
  }

  def getVoyageClusters(voyages: RDD[(String,ArrayBuffer[AISPosition])], numPartitions: Int, saveToFile: Boolean): Unit/*RDD[(String,ArrayBuffer[(ClusterStatistics,Polygon)])]*/ = {

    val st = new SpatialToolkit
    val voyageHulls = voyages.keyBy(_._1).partitionBy(new HashPartitioner(numPartitions)).glom().flatMap {
      arr =>
        arr.map(_._2).toMap.map(x => (x._1,x._2.filter(p => !Global._waypoints.exists(x => x._1.isInside(GeoPoint(p.longitude,p.latitude))))))
          .filter(y => y._2.size > 0).map { // remove empty voyages
          case (voyageId,positions) =>
            val dbscan = new DBScan(positions, 0.03, 10)
            val clusters = dbscan.getTrajectoryClusters//.filterNot(x => x.map(_.mmsi).distinct.size == 1) // remove clusters that contain one vessel
            /*val convexHulls = clusters.filter(_.size > 10).map{ // keep clusters that contain more than ten positions
              cluster =>
                (getClusterStats(cluster),st.getConvexHull(cluster.map(p => GeoPoint(p.longitude,p.latitude)).toList))
            }*/


            (voyageId,clusters.filter(_.size > 10))
        }
    }
    if (saveToFile) {
      /*println("Saving results to file...")
      val w = new FileWriter("voyage_clusters_new.csv")
      w.write("VOYAGE_ID,POLYGON,START_LON,START_LAT,END_LON,END_LAT,AVG_COG,AVG_SPEED,SPEED_DEVIATION,COG_DEVIATION\n")
      voyageHulls.collect.foreach {
        case (voyageId, polygons) =>
          polygons.foreach(p => w.write(s"$voyageId,${p._2.toString},${p._1.toString}\n"))
      }
      w.close()
      println("Save complete.")*/

      println("Saving results to file...")
      val w = new FileWriter("voyage_clusters_new.csv")
      w.write("VOYAGE_ID,CLUSTER_ID,MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SOG,TIMESTAMP,NAME,SHIP_TYPE,DESTINATION\n")
      voyageHulls.collect.foreach {
        case (voyageId, clusters) =>
          val test = clusters.zipWithIndex

          test.foreach{
            case (positions,clusterId) =>
              positions.foreach(p => w.write(s"$voyageId,$clusterId,${p.toString}\n"))
          }
      }
      w.close()
      println("Save complete.")
    }
    //voyageHulls
  }

  def getVoyageClustersFromFile(filename: String, sc: SparkContext, numPartitions: Int, saveToFile: Boolean): Unit/*RDD[(String,ArrayBuffer[(ClusterStatistics,Polygon)])]*/ = {
    val gc = new GlasseasContext
    val data = gc.readVoyageData(filename,sc)
    getVoyageClusters(data,numPartitions,saveToFile)
  }

  private def getClusterStats(cluster: ArrayBuffer[AISPosition]): ClusterStatistics = {
        val numElements = cluster.size
        var startLon = 0.0
        var startLat = 0.0
        var endLon = 0.0
        var endLat = 0.0
        val avgSpeed = cluster.map(_.speed).sum / numElements
        val avgCog = cluster.map(_.cog).sum / numElements
        val speedStandardDeviation = Math.sqrt(cluster.map(x => Math.pow(x.speed - avgSpeed, 2)).sum / numElements)
        val cogStandardDeviation = Math.sqrt(cluster.map(x => Math.pow(x.cog - avgCog, 2)).sum / numElements)
        if (avgCog >= 0 && avgCog <= 90) {
          startLon = cluster.map(_.longitude).min
          endLon = cluster.map(_.longitude).max
          startLat = cluster.map(_.latitude).min
          endLat = cluster.map(_.latitude).max
        }
        else if (avgCog > 90 && avgCog <= 180) {
          startLon = cluster.map(_.longitude).min
          startLat = cluster.map(_.latitude).max
          endLon = cluster.map(_.longitude).max
          endLat = cluster.map(_.latitude).min
        }
        else if (avgCog > 180 && avgCog <= 270) {
          startLon = cluster.map(_.longitude).max
          endLon = cluster.map(_.longitude).min
          startLat = cluster.map(_.latitude).max
          endLat = cluster.map(_.latitude).min
        }
        else {
          startLon = cluster.map(_.longitude).max
          endLon = cluster.map(_.longitude).min
          startLat = cluster.map(_.latitude).min
          endLat = cluster.map(_.latitude).max
        }
    ClusterStatistics(startLon,startLat,endLon,endLat,avgCog,avgSpeed,speedStandardDeviation,cogStandardDeviation)
  }

}
