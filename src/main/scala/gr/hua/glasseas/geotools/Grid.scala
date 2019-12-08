package gr.hua.glasseas.geotools

import java.io.FileWriter

/**
  *
  * @param minLon  The longitude of the reference point of the grid. Recommended value: -180.0
  * @param minLat  The latitude of the reference point of the grid. Recommended value: -90.0
  * @param maxLon The maximum longitude of the grid.
  * @param maxLat The maximum latitude of the grid.
  * @param stepLon horizontal size of the cell
  * @param stepLat vertical size of the cell
  * @param cells   list of cells
  */
class Grid(val minLon: Double, val minLat: Double, val maxLon: Double, val maxLat: Double, val stepLon: Double, val stepLat: Double, val cells: Map[GeoPoint,Cell]) {

  // assumption: Lon/Lat have at most 6 decimal digits
  private val MU = 1000000

  def this(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, stepLon: Double, stepLat: Double) {
    this(minLon,minLat,maxLon,maxLat,stepLon,stepLat,Grid.createGrid(minLon,minLat,maxLon,maxLat,stepLon,stepLat))
  }

  /**
    * Returns the enclosing cell based on the geo-point given
    * @param p geo-point
    * @return
    */
  def getEnclosingCell(p: GeoPoint): Option[Cell] = {
    val x = Math.round(p.longitude * MU)
    val y = Math.round(p.latitude * MU)
    val xStart = Math.round(minLon * MU)
    val xStep = Math.round(stepLon * MU)
    val yStart = Math.round(minLat * MU)
    val yStep = Math.round(stepLat * MU)
    val llx = x - ((x - xStart) % xStep)
    val lly = y - ((y - yStart) % yStep)
    val lowLeftX = llx.toDouble / MU
    val lowLeftY = lly.toDouble / MU
    cells.get(GeoPoint(lowLeftX,lowLeftY)) match {
      case Some(cell) => Some(cell)
      case None => None
    }
  }

  def save(filename: String): Unit = {
    val writer = new FileWriter(filename)
    writer.write("ID,LONGITUDE,LATITUDE\n")
    cells.map(_._2).foreach(cell => writer.write(s"${cell.id},${cell.geoPoint.longitude},${cell.geoPoint.latitude}\n"))
    writer.close
  }

}

object Grid {
  /**
    * Creates a grid based on the parameters given
    * @param maxLon
    * @param maxLat
    */
  def createGrid(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, stepLon: Double, stepLat: Double): Map[GeoPoint,Cell] = {
    var tempCells: Map[GeoPoint,Cell] = Map()
    var cellId = 1
    for (y <- minLat until maxLat by stepLat) {
      for (x <- minLon until maxLon by stepLon) {
        val lon = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val lat = BigDecimal(y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        tempCells += (GeoPoint(lon,lat) -> Cell(cellId,GeoPoint(lon,lat)))
        if (lon != (maxLon-stepLon)) cellId += 1
      }
      cellId += 1
    }
    tempCells
  }
}