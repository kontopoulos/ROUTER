package gr.hua.glasseas

import java.io.FileWriter

import gr.hua.glasseas.geotools.{Cell, GeoPoint, Grid, SpatialToolkit}
import gr.hua.glasseas.ml.clustering.DBScan

import scala.collection.mutable.ArrayBuffer

object WaypointsApp {

  def main(args: Array[String]): Unit = {

    LocalDatabase.initializeDefaults()
    LocalDatabase.updateGrid(new Grid(-8.0,29.0,38.2,47.2,0.01,0.01))
    LocalDatabase.grid.save("for_ports_grid.csv")

    var positionsPerCell: Map[Cell,Set[Int]] = Map()

    val gc = new GlasseasContext
    val st = new SpatialToolkit

    val inputValues = gc.readStream("dataset.csv").filter{
      p =>
        if (p.speed == 0.0) {
          val cell = LocalDatabase.grid.getEnclosingCell(GeoPoint(p.longitude, p.latitude))
          positionsPerCell.get(cell) match {
            case Some(set) =>
              if (!set.contains(p.id)) {
                val newSet = set ++ Set(p.id)
                positionsPerCell += (cell -> newSet)
                true
              }
              else false
            case None =>
              positionsPerCell += (cell -> Set(p.id))
              true
          }
        }
        else false
    }.to[ArrayBuffer]

    val inputValuesWriter = new FileWriter("input_values.csv")
    inputValuesWriter.write(s"MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SOG,TIMESTAMP,NAME,SHIP_TYPE,DESTINATION,ANNOTATION\n")
    inputValues.foreach(p => inputValuesWriter.write(s"$p\n"))
    inputValuesWriter.close()

    val dbscan = new DBScan(inputValues,500.0,10)
    val convexHulls = dbscan.getClusters.map(cluster => st.getConvexHull(cluster.map(p => GeoPoint(p.longitude,p.latitude)).toList)).zipWithIndex
    val polygonWriter = new FileWriter("waypoints.csv")
    polygonWriter.write("ID,POLYGON\n")
    convexHulls.foreach(ch => polygonWriter.write(s"${ch._2},${ch._1}\n"))
    polygonWriter.close()

  }

}
