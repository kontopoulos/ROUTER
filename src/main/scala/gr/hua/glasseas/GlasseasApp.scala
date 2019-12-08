package gr.hua.glasseas

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Locale, TimeZone, UUID}

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
import gr.hua.glasseas.geotools.{AISPosition, CartesianPoint, Cell, GeoPoint, Grid, Polygon, Preprocessor, SpatialToolkit}
import gr.hua.glasseas.ml.clustering.DBScan
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GlasseasApp {

  def main(args: Array[String]): Unit = {

    /*LocalDatabase.initializeDefaults()
    LocalDatabase.readConvexHulls("convexhulls_600.csv")
    val cellIds = LocalDatabase.convexHullsPerIndex.keys.toSet

    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val gc = new GlasseasContext
    val data = gc.readData("testing_no_waypoints.csv",sc)
    val totalPositions = data.count().toDouble
    val inCells = data.filter(p => cellIds.contains(LocalDatabase.grid.getEnclosingCell(GeoPoint(p.longitude,p.latitude)).get.id)).count().toDouble
    println(totalPositions)
    println(inCells)
    println(inCells/totalPositions)*/


    LocalDatabase.initializeDefaults()
    LocalDatabase.readConvexHulls("convexhulls_180.csv")

    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val gc = new GlasseasContext
    val data = gc.readVoyageData("testing_Cargo_voyages.csv",sc)//.filter(p => LocalDatabase.getEnclosingWaypoint(GeoPoint(p.longitude,p.latitude)).isEmpty)
    val totalPositions = data.count().toDouble
    val inHulls = data.filter{
      case (itinerary,p) =>
        LocalDatabase.convexHulls
         LocalDatabase.getEnclosingConvexHull(GeoPoint(p.longitude,p.latitude)) match {
           case Some(hull) =>
             val hullItinerary = hull._2.split("-").last.split("@").head
             if (hullItinerary == itinerary) true else false
           case None => false
         }
    }.count().toDouble
    println(totalPositions)
    println(inHulls)
    println(inHulls/totalPositions)




    /*LocalDatabase.initializeDefaults()

    val gc = new GlasseasContext
    val w = new FileWriter("testing_no_waypoints.csv")
    w.write("MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SOG,TIMESTAMP,NAME,SHIP_TYPE,DESTINATION,ANNOTATION\n")
    gc.readStream("testing.csv").foreach{
      p =>
        if (LocalDatabase.getEnclosingWaypoint(GeoPoint(p.longitude,p.latitude)).isEmpty) w.write(p + "\n")
    }
    w.close()*/

    /*LocalDatabase.initializeDefaults()
    LocalDatabase.readConvexHulls("convexhulls_180.csv")
    val gc = new GlasseasContext
    var idx = 0.0
    var truePositives = 0.0
    gc.readStream("testing_no_waypoints.csv").foreach{
      p =>
        idx += 1.0
        if (LocalDatabase.getEnclosingConvexHull(GeoPoint(p.longitude,p.latitude)).nonEmpty) truePositives += 1.0
    }
    println(idx)
    println(truePositives)
    println(truePositives/idx)*/


    /*val st = new SpatialToolkit

    val lons = Array(-2.427067,-2.4024329,2.190433)
    val lats = Array(36.338089,36.341011,41.299)
    val x: Array[Double] = Array.fill(lons.length)(0.0)
    val y: Array[Double] = Array.fill(lats.length)(0.0)
    val z: Array[Double] = Array.fill(lats.length)(0.0)

    for (i <- 0 until lons.length) {
      val gp = GeoPoint(lons(i),lats(i))
      val cp = st.geodeticToCartesian(gp)
      x(i) = cp.x
      y(i) = cp.y
      z(i) = cp.z
    }

    val lonPoint = 2.0
    val latPoint = 37.0
    val xPoint = st.geodeticToCartesian(GeoPoint(lonPoint,latPoint)).x

    val yPoint = st.lagrangeInterpolation(lons,lats,lonPoint)

    val g = st.cartesianToGeodetic(CartesianPoint(xPoint,yPoint,z(1)))
    //println(g)



    println(s"x = $lonPoint, y = $yPoint")*/

    /*val test = new FileWriter("test_projection_1.csv")
    test.write("MMSI,IMO,LATITUDE,LONGITUDE,COG,HEADING,SOG,TIMESTAMP,NAME,SHIP_TYPE,DESTINATION,ANNOTATION\n")

    val timeIncrement = 180

    val st = new SpatialToolkit
    val gc = new GlasseasContext
    val stream = gc.readStream("test_case_1.csv").toList//.filter(p => p.seconds >= 1439537367 && p.seconds <= 1439584177)
    val interpolated: ArrayBuffer[AISPosition] = ArrayBuffer()
    stream.groupBy(p => (p.longitude,p.latitude)).filter(_._2.size == 1).values.flatten.toList.sortBy(_.seconds).sliding(3,1).foreach{
      case positions =>
        //    stream.foreach(println)
        val second = positions(1)
        val third = positions.last

        val x = positions.map(_.longitude).toArray
        val y = positions.map(_.latitude).toArray

        val timeDiff = Math.abs(second.seconds - third.seconds)

        //    val timeDiff = 46810
        //    val timeDiff = 800
        val numIncrements = timeDiff/timeIncrement - 1

        val longitudeDiff =
          if (second.longitude < 0 && third.longitude < 0) Math.abs(Math.abs(second.longitude) - Math.abs(third.longitude))
          else if (second.longitude > 0 && third.longitude > 0) Math.abs(second.longitude - third.longitude)
          else Math.abs(second.longitude) + Math.abs(third.longitude)
        val longitudeIncrement = longitudeDiff/numIncrements

        if (timeDiff > 360 && longitudeDiff != 0.0) {
          var currentIncrement = 1
          var currentLongitude = second.longitude + longitudeIncrement
          var currentSeconds = second.seconds + timeIncrement
          var previousLongitude = second.longitude
          var previousLatitude = second.latitude
          while (currentIncrement != numIncrements) {
            val interpolatedLatitude = st.lagrangeInterpolation(x,y,currentLongitude)
            val distance = st.getHaversineDistance(GeoPoint(previousLongitude,previousLatitude),GeoPoint(currentLongitude,interpolatedLatitude))
            val currentSpeed = distance/0.05/1.852 // convert to knots

            val date = new Date(currentSeconds*1000);
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val formattedDate = sdf.format(date)

            val dl = if (previousLongitude < 0 && currentLongitude < 0) Math.abs(Math.abs(previousLongitude) - Math.abs(currentLongitude))
              else if (previousLongitude > 0 && currentLongitude > 0) Math.abs(previousLongitude - currentLongitude)
              else Math.abs(previousLongitude) + Math.abs(currentLongitude)
            val X = Math.cos(interpolatedLatitude.toRadians)*Math.sin(dl.toRadians)
            val Y = Math.cos(previousLatitude.toRadians)*Math.sin(interpolatedLatitude.toRadians) - Math.sin(previousLatitude.toRadians)*Math.cos(interpolatedLatitude.toRadians)*Math.cos(dl.toRadians)
            val bearing = Math.atan2(X,Y).toDegrees.toInt.toDouble

            interpolated.append(AISPosition(second.id,second.imo,interpolatedLatitude,currentLongitude,bearing,bearing,(currentSpeed*10).toInt/10.0,currentSeconds,formattedDate,second.shipname,second.shipType,second.destination,second.annotation))
            previousLongitude = currentLongitude
            previousLatitude = interpolatedLatitude
            currentLongitude += longitudeIncrement
            currentSeconds += timeIncrement
            currentIncrement += 1
          }
        }
    }
    interpolated.foreach(p => test.write(p + "\n"))
    test.close()*/


    /*val st = new SpatialToolkit
    val (a,b,c) = st.polyRegression(Seq((2.0,1.0),(4.0,4.0),(6.0,2.0)))
    println(s"y = $a + ${b}x + ${c}x^2")
    println(st.interpolate(2.0,a,b,c))*/

    /*
 *Solving three variable linear equation system
 * 3x + 2y -  z =  1 ---&gt; Eqn(1)
 * 2x - 2y + 4z = -2 ---&gt; Eqn(2)
 * -x + y/2-  z =  0 ---&gt; Eqn(3)
 */

    //Creating  Arrays Representing Equations
    /*val lhsArray = Array(Array(3.0, 2.0, -1.0), Array(2.0, -2.0, 4.0), Array(-1.0, 0.5, -1.0))
    val rhsArray = Array(1.0, -2.0, 0.0)
    //Creating Matrix Objects with arrays
    val lhs = new Matrix(lhsArray)
    val rhs = new Matrix(rhsArray, 3)
    //Calculate Solved Matrix
    val ans = lhs.solve(rhs);
    //Printing Answers
    System.out.println("x = " + Math.round(ans.get(0, 0)));
    System.out.println("y = " + Math.round(ans.get(1, 0)));
    System.out.println("z = " + Math.round(ans.get(2, 0)));*/

    /*val gp = GeoPoint(23.617884817336883,37.935926630470185)
    val st = new SpatialToolkit
    val cp = st.geodeticToCartesian(gp)
    println(gp)
    println(cp)
    println(st.cartesianToGeodetic(cp))
    println("=============")
    val gp2 = GeoPoint(2.8680394,-54.072878)
    val cp2 = st.geodeticToCartesian(gp2)
    println(gp2)
    println(cp2)
    println(st.cartesianToGeodetic(cp2))*/

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

//    System.setProperty("hadoop.home.dir","C:\\hadoop" )
//    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//
//    LocalDatabase.initializeDefaults()
    //Global._grid.save("grid.csv")


    /*var numCells: Map[Int,Set[String]] = Map()
    var its: Map[String,Set[String]] = Map()
    val w = new FileWriter("laslas.csv")
    scala.io.Source.fromFile("med_trips/trips.csv").getLines().drop(1).filter{
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
        w.write(line + "," + cell.id + "\n")
    }
    /*val t = numCells.filter(_._2.size > 1)
    val w = new FileWriter("routes.csv")
    w.write("LONGITUDE,LATITUDE,ID,ITINERARY\n")
    Global._grid.cells.filter(x => t.contains(x._2.id)).foreach{
      case (gp,c) =>
        t(c.id).foreach{
          x =>
            its(x).foreach(y => w.write(s"$gp,${x},${y}\n"))
        }
    }*/
    w.close()*/
//    val pr = new Preprocessor
//    val routes = pr.extractRoutes("dataset.csv","Cargo",sc,8,true)

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

        if (ports.exists(p => st.getHaversineDistance(GeoPoint(lon, lat), GeoPoint(p._1, p._2)) <= 2.0) && speed <= 0.0) {
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

}
