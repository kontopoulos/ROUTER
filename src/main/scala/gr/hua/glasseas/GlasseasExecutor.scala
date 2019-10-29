package gr.hua.glasseas

import java.io.FileWriter
import java.util
import java.util.UUID

import de.lmu.ifi.dbs.elki.algorithm.clustering.DBSCAN
import de.lmu.ifi.dbs.elki.data.NumberVector
import de.lmu.ifi.dbs.elki.data.`type`.TypeUtil
import de.lmu.ifi.dbs.elki.database.StaticArrayDatabase
import de.lmu.ifi.dbs.elki.datasource.ArrayAdapterDatabaseConnection
import de.lmu.ifi.dbs.elki.distance.distancefunction.geo.LngLatDistanceFunction
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.RTreeSettings
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.rstar.RStarTreeFactory
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.strategies.bulk.SortTileRecursiveBulkSplit
import de.lmu.ifi.dbs.elki.math.geodesy.WGS84SpheroidEarthModel
import de.lmu.ifi.dbs.elki.persistent.MemoryPageFileFactory
import gr.hua.glasseas.geotools.{Cell, GeoPoint, Grid, Polygon, Preprocessor, SpatialToolkit}
import gr.hua.glasseas.ml.clustering.DBScan
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GlasseasExecutor {

  def main(args: Array[String]): Unit = {

    /*val list = scala.io.Source.fromFile("test.csv").getLines().drop(1).map{
      line =>
        val parts = line.split(",")

        val voyageId = parts.head
        val mmsi = parts(1).toInt
        val imo = if (parts(2) == "NULL") -1 else parts(2).toInt
        val lat = parts(3).toDouble
        val lon = parts(4).toDouble
        val cog = parts(5).toDouble
        val heading = if (parts(6) == "NULL") 0.0 else parts(6).toDouble
        val speed = parts(7).toDouble/10.0
        val seconds = 0
        val timestamp = parts(8)
        val shipName = parts(9)
        val typeName = parts(10)
        val destination = parts(11)

        AISPosition(mmsi,imo,lat,lon,cog,heading,speed,seconds,timestamp,shipName,typeName,destination)
    }.to[ArrayBuffer]

    val dbscan = new DBScan(list,0.0,10)
    val test = new FileWriter("polygons.csv",true)
    test.write("POLYGON\n")
    val clusters = dbscan.getTrajectoryClusters
    var label = 0
    val w = new FileWriter("clusters.csv",true)
    w.write("CLUSTER,MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SOG,TIMESTAMP,NAME,SHIP_TYPE,DESTINATION\n")
    clusters.foreach{
      clusters =>
        clusters.foreach(p => w.write(s"$label,${p.toString}\n"))
        label += 1
    }
    w.close()
    clusters.foreach{
      cl =>
        val t = cl.map(x => GeoPoint(x.longitude,x.latitude)).toList
        val st = new SpatialToolkit
        test.write(st.getConvexHull(t).toString+"\n")
    }
    test.close()*/

    /*val types = scala.io.Source.fromFile("shiptypes.csv").getLines().drop(1).map{
      line =>
        val parts = line.split("[,]")
        val shipId = parts(1).toInt
        val vesselType = parts.last
        (shipId,vesselType)
    }.toMap

    import java.util.TimeZone
    import java.text.SimpleDateFormat

    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.setTimeZone(TimeZone.getTimeZone("Europe/Istanbul"))
    println(df.format(1462050000000L))

    val w = new java.io.FileWriter("dataset.csv",true)
    w.write("MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SPEED,TIMESTAMP,SHIP_NAME,SHIP_TYPE,DESTINATION\n")
    scala.io.Source.fromFile("sorted.csv").getLines().foreach{
      line =>
        val parts = line.split(" ")
        val seconds = parts.head.toLong/1000
        val timestamp = time(seconds*1000)
        val mmsi = parts(1).toInt
        val lon = parts(2).toDouble
        val lat = parts(3).toDouble
        val speed = parts(4).toDouble
        val cog = parts(5).toDouble

        val ais = AISPosition(mmsi,-1,lat,lon,cog,cog,speed,seconds,timestamp,"NULL",types.getOrElse(mmsi,"NULL"),"NULL")
        w.write(ais.toString+"\n")
    }
    w.close()



    def time(millis: Long) = {
      import java.util.TimeZone
      import java.text.SimpleDateFormat

      val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      df.setTimeZone(TimeZone.getTimeZone("Europe/Istanbul"))
      df.format(millis)
    }*/

//    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
//    val sc = new SparkContext(conf)

    Global.initialize()
    //Global._grid.save("grid.csv")


    var numCells: Map[Int,Set[String]] = Map()
    var its: Map[String,Set[String]] = Map()
//    val w = new FileWriter("lala.csv")
    scala.io.Source.fromFile("one.csv").getLines().drop(1).filter{
      line =>
        val parts = line.split(",")
        val itinerary = parts(1)
        itinerary == "40_to_379" || itinerary == "40_to_420" || itinerary == "6_to_186"
    }.foreach{
      line =>
        val parts = line.split(",")
        val id = parts.head
        val itinerary = parts(1)
        val lon = parts(5).toDouble
        val lat = parts(4).toDouble
        val cell = Global._grid.getEnclosingCell(GeoPoint(lon,lat))
        its.get(id) match {
          case Some(it) =>
            val nit = it + itinerary
            its += (id -> nit)
          case None => its += (id -> Set(itinerary))
        }
        numCells.get(cell.id) match {
          case Some(s) =>
            val ns = s + id
            numCells += (cell.id -> ns)
          case None => numCells += (cell.id -> Set(id))
        }
        //w.write(line + "," + cell.id + "\n")
    }
    val t = numCells.filter(_._2.size > 1)
    val w = new FileWriter("routes.csv")
    w.write("LONGITUDE,LATITUDE,ID,ITINERARY\n")
    Global._grid.cells.filter(x => t.contains(x._2.id)).foreach{
      case (gp,c) =>
        t(c.id).foreach{
          x =>
            its(x).foreach(y => w.write(s"$gp,${x},${y}\n"))
        }
    }
    w.close()
//    val pr = new Preprocessor
//    val routes = pr.extractRoutes("dataset.csv","Cargo",sc,4,true)

//    pr.getVoyageClustersFromFile("one.csv",sc,4,true)


    //PKDDExperiment

    /*val st = new SpatialToolkit
    val wr = new FileWriter("ports/polygons_3.csv",true)
    wr.write("ID,POLYGON\n")
    scala.io.Source.fromFile("ports/clusters_3.csv").getLines.drop(1).map{
      line =>
        val parts = line.split(",")
        val id = parts.head.toInt
        val lon = parts(1).toDouble
        val lat = parts.last.toDouble
        (id,GeoPoint(lon,lat))
    }.toArray.groupBy(_._1).map{
      case (id,arr) =>
        (id,st.getConvexHull(arr.map(_._2).toList))
    }.foreach{
      case (id,p) =>
        wr.write(id + "," + p.toString+"\n")
    }
    wr.close()*/

    //extractPorts

    // lon: -180, lat: -90
    /*val cells = Grid.createGrid(-8.0,29.0,38.2,47.2,0.01,0.01)
    val g = new Grid(-8.0,29.0,38.2,47.2,0.01,0.01, cells)
    //g.save("grid.csv")
    var temp: Map[Cell,Set[Int]] = Map()
    val wr = new FileWriter("stop_points.csv",true)
    wr.write("MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SOG,TIMESTAMP,NAME,SHIP_TYPE,DESTINATION\n")

    val gc =  new GlasseasContext
    gc.readStream("humanitarian_hd_201508.csv").foreach{
      position =>

        val cell = g.getEnclosingCell(GeoPoint(position.longitude,position.latitude))
        temp.get(cell) match {
          case Some(list) => {
            if (!list.contains(position.mmsi) && position.speed == 0.0) {
              var newList = list
              newList += position.mmsi
              temp += (cell -> newList)
              wr.write(position.toString + "\n")
            }
          }
          case None => {
            temp += (cell -> Set(position.mmsi))
            if (position.speed == 0.0) wr.write(position.toString + "\n")
          }
        }
    }
    wr.close()*/

    /*val cells = Grid.createGrid(-8.0,29.0,38.2,47.2,0.01,0.01)
    val g = new Grid(-8.0,29.0,38.2,47.2,0.01,0.01, cells)
    var temp: Map[Cell,Set[Int]] = Map()
    val gc =  new GlasseasContext
    val st = new SpatialToolkit
    var prev: Map[Int,AISPosition] = Map()
    val wr = new FileWriter("turn_points.csv",true)
    wr.write("MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SOG,TIMESTAMP,NAME,SHIP_TYPE,DESTINATION\n")
    gc.readStream("humanitarian_hd_201508.csv").foreach{
      position =>
        prev.get(position.mmsi) match {
          case Some(p) =>
            if (st.isTurningPoint(p,position,30.0)) {
              //wr.write(position.toString + "\n")
              val cell = g.getEnclosingCell(GeoPoint(position.longitude,position.latitude))
              temp.get(cell) match {
                case Some(list) =>
                  if (!list.contains(position.mmsi)) {
                    var newList = list
                    newList += position.mmsi
                    temp += (cell -> newList)
                    wr.write(position.toString + "\n")
                    wr.write(p.toString + "\n")
                  }
                case None => {
                  temp += (cell -> Set(position.mmsi))
                  wr.write(position.toString + "\n")
                  wr.write(p.toString + "\n")
                }
              }
            }
          case None =>
        }
        prev += (position.mmsi -> position)
    }
    wr.close()*/


    /*val gc = new GlasseasContext
    val inputValues = gc.readStream("turn_points.csv").toArray
    val cw = new java.io.FileWriter("turn_clusters.csv", true)
    cw.write("ID,POLYGON\n")

    val dbscan = new DBScan(collection.mutable.ArrayBuffer(inputValues: _*), 1000, 10)
    val clusters = dbscan.getClusters
    val st = new SpatialToolkit
    var label = 0
    clusters.foreach {
      case cluster =>
        if (cluster.size >= 50 && cluster.size <= 1000) { // for turn only
          val polygon = st.getConvexHull(cluster.map(x => GeoPoint(x.longitude, x.latitude)).toList)
          cw.write(s"$label,${polygon.toString}\n")
        }
        label += 1
    }
    cw.close()*/

    /*val stopPoints = scala.io.Source.fromFile("stop_clusters_pol.csv").getLines().drop(1).map{
      x =>
        val p = new Polygon()
        val pol = p.fromWKTFormat(x)
        pol
    }.toSet
    val turnPoints = scala.io.Source.fromFile("turn_clusters_pol.csv").getLines().drop(1).map{
      x =>
        val p = new Polygon()
        val pol = p.fromWKTFormat(x)
        pol
    }.toSet

    val waypoints = stopPoints ++ turnPoints

    // -8.0,29.0,38.2,47.2
    val ports = scala.io.Source.fromFile("ports/ports.csv").getLines().drop(1).map{
      x =>
        val p = new Polygon()
        val pol = p.fromWKTFormat(x)
        pol
    }.toSet.filter{
      p =>
        p.points.forall(gp => gp.longitude >= 9.93 && gp.longitude <= 29.73 && gp.latitude >= 30.41 && gp.latitude <= 42.81)
    }

    println("Stop polygons: " + stopPoints.size)
    println("Turn polygons: " + turnPoints.size)
    println("Waypoints: " + waypoints.size)
    println("Ports: " + ports.size)
    var tp = 0.0
    var fn = 0.0
    ports.foreach{
      port =>
        var exists = false
        port.points.foreach{
          portPoint =>
            if (exists == false) {
              if (waypoints.exists(p => p.isInside(portPoint))) {
                exists = true
              }
            }
        }
        if (exists == true) tp += 1
        else fn += 1
    }
    println("TP: " + tp)
    println("FN: " + fn)
    println("Recall: " + tp/(tp+fn))*/


    /*val inputValues = scala.io.Source.fromFile("ports/filtered_5.csv").getLines.drop(1).map {
      line =>
        val parts = line.split("[ ]")
        val mmsi = parts(1).toInt
        val lon = parts(2).toDouble
        val lat = parts(3).toDouble

        AISPosition(mmsi, 0, lat, lon, 0, 0, 0, 0, "", "", "")
    }.toArray

    val numPoints = inputValues.length

    val data = Array.ofDim[Double](numPoints, 2)
    for (i <- 0 until numPoints) {
      data(i)(0) = inputValues(i).longitude
      data(i)(1) = inputValues(i).latitude
    }

    val db = new StaticArrayDatabase(new ArrayAdapterDatabaseConnection(data, null, 0), util.Arrays.asList(new RStarTreeFactory(new MemoryPageFileFactory(4096), new RTreeSettings(SortTileRecursiveBulkSplit.STATIC))))
    db.initialize()

    val rel = db.getRelation[NumberVector](TypeUtil.NUMBER_VECTOR_FIELD)

    val clusters = new DBSCAN(new LngLatDistanceFunction(WGS84SpheroidEarthModel.STATIC), 500.0, 10).run(db).getAllClusters.asScala

    val w = new FileWriter("ports/clusters_5.csv")
    var label = 0
    for (cluster <- clusters) {
      label += 1
      if (!cluster.isNoise) {
        val it = cluster.getIDs.iter()
        while (it.valid()) {
          val p = rel.get(it)
          val lon = p.doubleValue(0)
          val lat = p.doubleValue(1)
          w.write(s"$label,$lon,$lat\n")
          it.advance()
        }
      }
    }
    w.close()*/









    /*val inputValues = scala.io.Source.fromFile("ports/port_points.csv").getLines.drop(1).map{
      line =>
        val parts = line.split("[ ]")
        val mmsi = parts(1).toInt
        val lon = parts(2).toDouble
        val lat = parts(3).toDouble

        AISPosition(mmsi,0,lat,lon,0,0,0,0,"","","")
    }.to[ArrayBuffer]
    val dbscan = new DBScan(inputValues,2.0,10)
    val clusters = dbscan.getParallelClusters(6)*/
    /*var label = 0
    val cw = new FileWriter("ports/clusters.csv",true)
    clusters.foreach {
      case cluster =>
        cluster.foreach(p => cw.write(s"$label,${p.toString}\n"))
        label += 1
    }
    cw.close*/

  }

  def extractPorts: Unit = {

    val ports = scala.io.Source.fromFile("ports/world_ports.csv").getLines.drop(1).map {
      l =>
        val parts = l.split(",")
        val country = parts(1)
        val name = parts.head
        val lon = parts.last.toDouble
        val lat = parts(2).toDouble

        (lon, lat)
    }.filter {
      case (x, y) =>
        // -7.78,29.43,38.19,47.14
        if (x >= -7.78 && x <= 38.19 && y >= 29.43 && y <= 47.14) true
        else false
    }.toSet

    val st = new SpatialToolkit
    val w = new FileWriter("ports/port_points_2.csv", true)
    var q = new mutable.Queue[String]
    scala.io.Source.fromFile("ports/sorted.csv").getLines.foreach {
      line =>
        val parts = line.split("[ ]")
        val lon = parts(2).toDouble
        val lat = parts(3).toDouble
        val speed = parts(4).toDouble

        if (ports.exists(p => st.getHarvesineDistance(GeoPoint(lon, lat), GeoPoint(p._1, p._2)) <= 2.0) && speed <= 0.0) {
          q += line
          if (q.size == 50000) {
            val value = q.mkString("\n")
            w.write(value + "\n")
            q.dequeueAll(x => true)
          }
        }
    }
    val value = q.mkString("\n")
    w.write(value + "\n")
    q.dequeueAll(x => true)
    w.close

  }

  def PKDDExperiment: Unit = {
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
