package gr.hua.glasseas

import gr.hua.glasseas.geotools.{Cell, GeoPoint, Grid, Polygon}

object LocalDatabase {

  // a default grid (Mediterranean sea)
  var grid = new Grid(-8.0,29.0,38.2,47.2,0.2,0.2)
  private var waypoints: Map[Polygon,Int] = Map()
  private var waypointsPerIndex: Map[Int,Set[Polygon]] = Map()

  var convexHulls: Map[Polygon,String] = Map()
  var convexHullsPerIndex: Map[Int,Set[Polygon]] = Map()

  if (System.getProperty("os.name").toLowerCase.contains("windows")) System.setProperty("hadoop.home.dir","C:\\hadoop")

  def initializeDefaults(): Unit = {
    println("Initializing defaults...")
    readWaypoints("waypoints/training_Cargo_waypoints_2000.0_10.csv")
    println("Done!")
  }

  def initialize(waypointsFile: String): Unit = {
    println("Initializing...")
    readWaypoints(waypointsFile)
    println("Done!")
  }

  def updateGrid(grid: Grid): Unit = {
    synchronized{
      this.grid = grid
      createWaypointIndexes()
    }
  }

  def gridFromFile(filename: String, minLon: Double, minLat: Double, maxLon: Double,  maxLat: Double, stepLon: Double, stepLat: Double): Unit = {
    val gridCells = scala.io.Source.fromFile(filename).getLines().drop(1).map{
      line =>
        val columns = line.split(",")
        val id = columns.head.toInt
        val lon = columns(1).toDouble
        val lat = columns.last.toDouble
        val gp = GeoPoint(lon,lat)
        val cell = Cell(id,gp)
        (gp,cell)
    }.toMap
    val loadedGrid = new Grid(minLon,minLat,maxLon,maxLat,stepLon,stepLat,gridCells)
    synchronized {
      this.grid = loadedGrid
    }
  }

  def readWaypoints(filename: String): Unit = {
    synchronized {
      waypoints = scala.io.Source.fromFile(filename).getLines().drop(1)
        .map {
          line =>
            val columns = line.split(",")
            val id = columns.head.toInt
            val polygon = columns.drop(1).mkString(",")
            (new Polygon().fromWKTFormat(polygon), id)
        }.toMap
      createWaypointIndexes()
    }
  }

  def readConvexHulls(filename: String): Unit = {
    synchronized {
      convexHulls = scala.io.Source.fromFile(filename).getLines().drop(1)
        .map {
          line =>
            val columns = line.split(",")
            val id = columns.head
            val itinerary = columns(1)
            //val cid = columns(2)
            val polygon = columns.drop(2).mkString(",")
            (new Polygon().fromWKTFormat(polygon), id)
        }.toMap
      createConvexHullsIndexes()
    }
  }

  private def createWaypointIndexes(): Unit = {
    waypoints.foreach{
      case (waypoint,id) =>
        val cellsOfPort = waypoint.points.map(p => grid.getEnclosingCell(p)).distinct
        cellsOfPort.foreach{
          cell =>
            waypointsPerIndex.get(cell.get.id) match {
              case Some(set) =>
                val newSet = set ++ Set(waypoint)
                waypointsPerIndex += (cell.get.id -> newSet)
              case None => waypointsPerIndex += (cell.get.id -> Set(waypoint))
            }
        }
    }
  }

  private def createConvexHullsIndexes(): Unit = {
    convexHulls.foreach{
      case (convexHull,id) =>
        val cellsOfHulls = convexHull.points.map(p => grid.getEnclosingCell(p)).distinct
        cellsOfHulls.foreach{
          cell =>
            convexHullsPerIndex.get(cell.get.id) match {
              case Some(set) =>
                val newSet = set ++ Set(convexHull)
                convexHullsPerIndex += (cell.get.id -> newSet)
              case None => convexHullsPerIndex += (cell.get.id -> Set(convexHull))
            }
        }
    }
  }

  def getEnclosingWaypoint(p: GeoPoint): Option[(Polygon,Int)] = {
    grid.getEnclosingCell(p) match {
      case Some(cell) =>
        val gridIndexLength = Math.abs(grid.minLon) + Math.abs(grid.maxLon)/grid.stepLon
        val cellRight = cell.id + 1
        val cellLeft = cell.id - 1
        val cellTop = cell.id + gridIndexLength
        val cellTopRight = cellTop + 1
        val cellTopLeft = cellTop - 1
        val cellBottom = cell.id - gridIndexLength
        val cellBottomRight = cellBottom + 1
        val cellBottomLeft = cellBottom - 1
        val filteredWaypoints = waypointsPerIndex.filter(ppi => ppi._1 == cell.id || ppi._1 == cellRight || ppi._1 == cellLeft
          || ppi._1 == cellTop || ppi._1 == cellTopRight || ppi._1 == cellTopLeft
          || ppi._1 == cellBottom || ppi._1 == cellBottomRight || ppi._1 == cellBottomLeft)
        if (filteredWaypoints.isEmpty) None
        else {
          val jointWaypoints = filteredWaypoints.values.reduce(_ ++ _)
          jointWaypoints.find(wp => wp.isInside(p)) match {
            case Some(waypoint) => Some((waypoint,waypoints(waypoint)))
            case None => None
          }
        }
      case None => None
    }
  }

  def getEnclosingConvexHull(p: GeoPoint): Option[(Polygon,String)] = {
    grid.getEnclosingCell(p) match {
      case Some(cell) =>
        val gridIndexLength = Math.abs(grid.minLon) + Math.abs(grid.maxLon)/grid.stepLon
        val cellRight = cell.id + 1
        val cellLeft = cell.id - 1
        val cellTop = cell.id + gridIndexLength
        val cellTopRight = cellTop + 1
        val cellTopLeft = cellTop - 1
        val cellBottom = cell.id - gridIndexLength
        val cellBottomRight = cellBottom + 1
        val cellBottomLeft = cellBottom - 1
        val filteredConvexHulls = convexHullsPerIndex.filter(ppi => ppi._1 == cell.id || ppi._1 == cellRight || ppi._1 == cellLeft
          || ppi._1 == cellTop || ppi._1 == cellTopRight || ppi._1 == cellTopLeft
          || ppi._1 == cellBottom || ppi._1 == cellBottomRight || ppi._1 == cellBottomLeft)
        if (filteredConvexHulls.isEmpty) None
        else {
          val jointConvexHulls = filteredConvexHulls.values.reduce(_ ++ _)
          jointConvexHulls.find(ch => ch.isInside(p)) match {
            case Some(convexHull) => Some((convexHull,convexHulls(convexHull)))
            case None => None
          }
        }
      case None => None
    }
  }

}
