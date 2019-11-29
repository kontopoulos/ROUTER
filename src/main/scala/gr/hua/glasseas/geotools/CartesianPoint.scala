package gr.hua.glasseas.geotools

case class CartesianPoint(x: Double, y: Double, z: Double) {

  override def toString: String = s"$x,$y,$z"

}
