package gr.hua.glasseas

import gr.hua.glasseas.geotools.{Cell, GeoPoint, Grid, Polygon}

object Global {

  // an example of a grid
  var _grid = new Grid(-8.0,29.0,38.2,47.2,0.2,0.2)
  var _ports: Map[Polygon,Int] = Map()
  var _waypoints: Set[(Polygon,Int)] = Set()
  private var portsPerIndex: Map[Int,Set[Polygon]] = Map()

  def initialize(): Unit = {
    println("Initializing grid...")
    //updateGrid(new Grid(-8.0,29.0,38.2,47.2,0.01,0.01))
    println("Reading ports...")
    updatePorts("ports/ports.csv")
    //updateWaypoints("ports/waypoints.csv")
  }

  def updateGrid(grid: Grid): Unit = {
    synchronized{
      this._grid = grid
    }
  }

  def updatePorts(filename: String): Unit = {
    val ports = scala.io.Source.fromFile(filename).getLines().drop(1).map(line => new Polygon().fromWKTFormat(line)).toSet
    _ports = ports.zipWithIndex.toMap
    ports.foreach{
      port =>
        val cellsOfPort = port.points.map(p => _grid.getEnclosingCell(p)).distinct
        cellsOfPort.foreach{
          cell =>
            portsPerIndex.get(cell.id) match {
              case Some(set) =>
                val newSet = set ++ Set(port)
                portsPerIndex += (cell.id -> newSet)
              case None => portsPerIndex += (cell.id -> Set(port))
            }
        }
    }
  }

  def updateWaypoints(filename: String): Unit = {
    val turnWaypoints = scala.io.Source.fromFile("ports/turn_clusters_pol.csv").getLines().drop(1).map(line => new Polygon().fromWKTFormat(line)).toSet
    val stopWaypoints = scala.io.Source.fromFile("ports/stop_clusters_pol.csv").getLines().drop(1).map(line => new Polygon().fromWKTFormat(line)).toSet
    /*val waypoints = turnWaypoints ++ stopWaypoints
    this._waypoints = waypoints.zipWithIndex*/
    synchronized{
      this._waypoints = scala.io.Source.fromFile(filename).getLines().drop(1).map(line => new Polygon().fromWKTFormat(line)).toSet.zipWithIndex
    }
  }

  def getEnclosingPort(p: GeoPoint): Option[(Polygon,Int)] = {
    val cell = _grid.getEnclosingCell(p)
    val gridIndexLength = Math.abs(_grid.minLon) + Math.abs(_grid.maxLon)/_grid.stepLon
    val cellRight = cell.id + 1
    val cellLeft = cell.id - 1
    val cellTop = cell.id + gridIndexLength
    val cellTopRight = cellTop + 1
    val cellTopLeft = cellTop - 1
    val cellBottom = cell.id - gridIndexLength
    val cellBottomRight = cellBottom + 1
    val cellBottomLeft = cellBottom - 1
    val filteredPorts = portsPerIndex.filter(ppi => ppi._1 == cell.id || ppi._1 == cellRight || ppi._1 == cellLeft
      || ppi._1 == cellTop || ppi._1 == cellTopRight || ppi._1 == cellTopLeft
      || ppi._1 == cellBottom || ppi._1 == cellBottomRight || ppi._1 == cellBottomLeft)
    if (filteredPorts.isEmpty) None
    else {
      val combinedPorts = filteredPorts.values.reduce(_ ++ _)
      combinedPorts.find(prt => prt.isInside(p)) match {
        case Some(port) => Some((port,_ports(port)))
        case None => None
      }
    }
  }

}
