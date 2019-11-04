package gr.hua.glasseas.geotools

import scala.collection.mutable.ArrayBuffer

case class Cluster(clusterId: String, positions: ArrayBuffer[AISPosition]) {

  override def toString: String = positions.map(p => s"$clusterId,$p").mkString("\n")

}
