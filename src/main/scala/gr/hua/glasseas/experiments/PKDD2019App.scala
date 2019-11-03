package gr.hua.glasseas.experiments

import gr.hua.glasseas.GlasseasContext
import gr.hua.glasseas.ml.clustering.DBScan

object PKDD2019App {

  def main(args: Array[String]): Unit = {
    val gc = new GlasseasContext
    val inputValues = gc.readStream("waypoints/reduced_routes_piraeus.csv").toArray

    val dbscan = new DBScan(collection.mutable.ArrayBuffer(inputValues: _*), 0.03, 10)
    val clusters = dbscan.getTrajectoryClusters

    val cw = new java.io.FileWriter("waypoints/cl_03_5.csv", true)
    cw.write("ID,MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SPEED,TIMESTAMP,SHIPNAME,SHIPTYPE,DESTINATION\n")
    var label = 0
    val stats = new java.io.FileWriter("waypoints/cl_03_5_stats.csv", true)
    stats.write("ID,LATITUDE,LONGITUDE,AVGHEADING,AVGSPEED,SPEEDDEVIATION,HEADINGDEVIATION\n")

    clusters.foreach {
      case cluster =>
        cluster.foreach(p => cw.write(s"$label,${p.toString}\n"))
        val numElements = cluster.size
        var startLon = 0.0
        var startLat = 0.0
        var endLon = 0.0
        var endLat = 0.0
        val avgSpeed = cluster.map(_.speed).reduce(_ + _) / numElements
        val avgCog = cluster.map(_.cog).reduce(_ + _) / numElements
        val speedStandardDeviation = Math.sqrt(cluster.map(x => Math.pow(x.speed - avgSpeed, 2)).reduce(_ + _) / numElements)
        val cogStandardDeviation = Math.sqrt(cluster.map(x => Math.pow(x.cog - avgCog, 2)).reduce(_ + _) / numElements)
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
        stats.write(s"$label,$startLat,$startLon,$avgCog,$avgSpeed,$speedStandardDeviation,$cogStandardDeviation\n")
        stats.write(s"$label,$endLat,$endLon,$avgCog,$avgSpeed,$speedStandardDeviation,$cogStandardDeviation\n")
        label += 1
    }
    cw.close
    stats.close
  }

}
