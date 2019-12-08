package gr.hua.glasseas

import gr.hua.glasseas.geotools.{Cluster, GeoPoint, SpatialToolkit}
import gr.hua.glasseas.ml.clustering.DBScan
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ConvexHullsSparkApp {

  def main(args: Array[String]): Unit = {

    val filename = "training_voyages_interpolated_180_noports.csv"

    LocalDatabase.initializeDefaults()

    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val gc = new GlasseasContext
    val data = gc.readVoyageData(filename,sc)


    val trajectoryClusters = data.filter(x => LocalDatabase.grid.getEnclosingCell(GeoPoint(x._2.longitude,x._2.latitude)).nonEmpty).map(record => (record._1,LocalDatabase.grid.getEnclosingCell(GeoPoint(record._2.longitude,record._2.latitude)).get,record._2))
      .groupBy(record => (record._1,record._2))
      .flatMap{
        case (groupId,records) =>
          val itinerary = groupId._1
          val cell = groupId._2
          val positions = records.map(_._3).to[ArrayBuffer]
          val st = new SpatialToolkit

          val meanHeading = positions.map(_.cog).sum/positions.size
          val stdHeading = Math.sqrt(positions.map(p => Math.pow(Math.abs(p.cog-meanHeading),2)).sum/positions.size)

          val meanSpeed = positions.map(_.speed).sum/positions.size
          val stdSpeed = Math.sqrt(positions.map(p => Math.pow(Math.abs(p.speed-meanSpeed),2)).sum/positions.size)

          val distances = positions.map(p => GeoPoint(p.longitude,p.latitude)).sliding(2,1).map(list => st.getHaversineDistance(list.head,list.last)).toList
          val meanDistance = distances.sum/distances.size
          val stdDistance = Math.sqrt(distances.map(d => Math.pow(Math.abs(d-meanDistance),2)).sum/distances.size)

          val dbscan = new DBScan(positions, 0.03,stdDistance,stdSpeed,stdHeading, 6)
          val clusters = dbscan.getTrajectoryClusters
            .zipWithIndex
            .map{ case (clusterPositions,clusterIndex) => Cluster(s"${clusterIndex}-${itinerary}@${cell.id}",itinerary,cell,clusterPositions) }
          clusters
    }

    val convexHulls = trajectoryClusters.filter(cl => cl.positions.nonEmpty && cl.positions.map(_.annotation).distinct.size > 1)
      .map{
        cluster =>
          val st = new SpatialToolkit
          val convexHull = st.getConvexHull(cluster.positions.map(p => GeoPoint(p.longitude,p.latitude)).toList)
          s"${cluster.clusterId},${cluster.itinerary},${cluster.cell.id},$convexHull"
      }
    convexHulls.saveAsTextFile(s"$filename-convexhulls")

  }

}
