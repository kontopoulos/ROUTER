package gr.hua.glasseas.geotools

import scala.collection.mutable.ArrayBuffer

case class Cluster(clusterId: String, itinerary: String, cell: Cell, positions: ArrayBuffer[AISPosition]) {

  override def toString: String = positions.map(p => s"$clusterId,$itinerary,${cell.id},$p").mkString("\n")

}
