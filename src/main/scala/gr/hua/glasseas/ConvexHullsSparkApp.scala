package gr.hua.glasseas

import gr.hua.glasseas.geotools.{Cluster, GeoPoint, SpatialToolkit}
import gr.hua.glasseas.ml.clustering.DBScan
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ConvexHullsSparkApp {

  def main(args: Array[String]): Unit = {

    val filename = "dataset_voyages.csv"
    val numPartitions = 8

    System.setProperty("hadoop.home.dir","C:\\hadoop" )
    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val gc = new GlasseasContext
    val data = gc.readVoyageData(filename,sc)


    val trajectoryClusters = data.map(record => (record._1,LocalDatabase.grid.getEnclosingCell(GeoPoint(record._2.longitude,record._2.latitude)),record._2))
      .groupBy(record => (record._1,record._2))
      .flatMap{
        case (groupId,records) =>
          val itinerary = groupId._1
          val cell = groupId._2
          val positions = records.map(_._3).to[ArrayBuffer]
          val dbscan = new DBScan(positions, 0.03, 5)
          val clusters = dbscan.getTrajectoryClusters
            .zipWithIndex
            .map{
              case (clusterPositions,clusterIndex) =>
                Cluster(s"${clusterIndex}-${itinerary}@${cell.id}",clusterPositions)
            }
          clusters
    }
    trajectoryClusters.saveAsTextFile(s"$filename-clusters")

    /*val convexHulls = trajectoryClusters.groupBy(_._1)
      .map{
        case (group,positions) =>
          val st = new SpatialToolkit
          val convexHull = st.getConvexHull(positions.map(kp => GeoPoint(kp._2.longitude,kp._2.latitude)).toList)
          s"$group,$convexHull"
      }
    convexHulls.cache()
    trajectoryClusters.saveAsTextFile(s"$filename-convexhulls")*/

  }

}
