package gr.hua.glasseas.geotools

case class Cell(id: Int, geoPoint: GeoPoint) {

  override def toString: String = s"$id ${geoPoint.longitude} ${geoPoint.latitude}"

  override def hashCode(): Int = id

}
