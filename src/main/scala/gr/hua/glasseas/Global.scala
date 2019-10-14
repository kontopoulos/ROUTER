package gr.hua.glasseas

import gr.hua.glasseas.geotools.{Grid, Polygon}

object Global {

  // an example of a grid
  var _grid = new Grid(-8.0,29.0,38.2,47.2,0.1,0.1)
  var _ports: Set[(Polygon,Int)] = Set()
  var _waypoints: Set[(Polygon,Int)] = Set()

  def initialize(): Unit = {
    println("Initializing grid...")
    //updateGrid(new Grid(-8.0,29.0,38.2,47.2,0.01,0.01))
    println("Reading ports...")
    updatePorts("ports/reduced_ports.csv")
    //updateWaypoints("ports/waypoints.csv")
  }

  def updateGrid(grid: Grid): Unit = {
    synchronized{
      this._grid = grid
    }
  }

  def updatePorts(filename: String): Unit = {
    synchronized{
      this._ports = scala.io.Source.fromFile(filename).getLines().drop(1).map(line => new Polygon().fromWKTFormat(line)).toSet.zipWithIndex
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

}
