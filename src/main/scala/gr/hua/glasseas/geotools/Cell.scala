package gr.hua.glasseas.geotools

case class Cell(id: Int, geoPoint: GeoPoint) {

  override def toString: String = s"$id ${geoPoint.longitude} ${geoPoint.latitude}"

  override def hashCode(): Int = {
    val tmp = (geoPoint.latitude + ((geoPoint.longitude + 1) / 2)).toInt
    Math.abs((geoPoint.longitude + (tmp * tmp)).toInt)
  }

}
