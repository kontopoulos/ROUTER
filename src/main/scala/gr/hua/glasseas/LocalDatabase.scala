package gr.hua.glasseas

import gr.hua.glasseas.geotools.{GeoPoint, Grid, Polygon}

object LocalDatabase {

  // a default grid (Mediterranean sea)
  var grid = new Grid(-8.0,29.0,38.2,47.2,0.2,0.2)
  private var waypoints: Map[Polygon,Int] = Map()
  private var waypointsPerIndex: Map[Int,Set[Polygon]] = Map()

  def initializeDefaults(): Unit = {
    println("Initializing defaults...")
    readWaypoints("waypoints/waypoints.csv")
    println("Done!")
  }

  def updateGrid(grid: Grid): Unit = {
    synchronized{
      this.grid = grid
      createWaypointIndexes()
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

  private def createWaypointIndexes(): Unit = {
    waypoints.foreach{
      case (waypoint,id) =>
        val cellsOfPort = waypoint.points.map(p => grid.getEnclosingCell(p)).distinct
        cellsOfPort.foreach{
          cell =>
            waypointsPerIndex.get(cell.id) match {
              case Some(set) =>
                val newSet = set ++ Set(waypoint)
                waypointsPerIndex += (cell.id -> newSet)
              case None => waypointsPerIndex += (cell.id -> Set(waypoint))
            }
        }
    }
  }

  def getEnclosingWaypoint(p: GeoPoint): Option[(Polygon,Int)] = {
    val cell = grid.getEnclosingCell(p)
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
  }

}
