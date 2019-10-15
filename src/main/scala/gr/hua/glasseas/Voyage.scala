package gr.hua.glasseas

import scala.collection.mutable.ArrayBuffer

case class Voyage(voyageId: String, itinerary: String, voyagePositions: ArrayBuffer[AISPosition]) {

  override def toString: String = voyagePositions.map(p => s"$voyageId,$itinerary,$p").mkString("\n")

}
