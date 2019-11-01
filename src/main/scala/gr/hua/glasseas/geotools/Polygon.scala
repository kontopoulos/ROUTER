package gr.hua.glasseas.geotools

import java.io.FileWriter

class Polygon(val points: List[GeoPoint]) extends Serializable {

  def this() {
    this(List[GeoPoint]())
  }

  override def toString: String = s""""POLYGON ((${points.map(gp => s"${gp.longitude} ${gp.latitude}").mkString(", ")}))""""

  def save(filename: String): Unit = {
    val writer = new FileWriter(filename)
    writer.write(this.toString)
    writer.close
  }

  def fromWKTFormat(line: String): Polygon = {
    val geoPoints = line.replace("\"POLYGON ((","")
      .replace("))\"","")
      .split(", ")
      .map{
        l =>
          val parts = l.split("[ ]")
          GeoPoint(parts.head.toDouble,parts.last.toDouble)
      }.toList
    new Polygon(geoPoints)
  }

  /**
    * Checks whether the given geo-point is located inside the current polygon
    * @param p geo-point
    * @return
    */
  def isInside(p: GeoPoint): Boolean = {
    (points.last :: points).sliding(2).foldLeft(false) {
      case (c, List(i, j)) =>
        val cond = {
          (
            (i.latitude <= p.latitude && p.latitude < j.latitude) ||
              (j.latitude <= p.latitude && p.latitude < i.latitude)
            ) &&
            (p.longitude < (j.longitude - i.longitude) * (p.latitude - i.latitude) / (j.latitude - i.latitude) + i.longitude)
        }
        if (cond) !c else c
    }
  }

  def getArea: Double = {
    var sum = 0.0
    for (i <- 0 until points.size) {
      sum = sum + (points.apply(i).longitude * points.apply(i+1).latitude) - (points.apply(i).latitude * points.apply(i+1).longitude)
    }
    sum
  }

}
