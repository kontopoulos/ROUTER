package gr.hua.glasseas.cluster.threads

import java.util.UUID

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import gr.hua.glasseas.geotools.{AISPosition, GeoPoint, Voyage}
import gr.hua.glasseas.{LocalDatabase, geotools}

import scala.collection.mutable.ArrayBuffer

object Ship {

  sealed trait Command
  case object End extends Command
  final case class aisMessage(aisPosition: AISPosition) extends Command

  val TypeKey = EntityTypeKey[Command]("Ship")

  def defaultThreadBehavior(id: String): Behavior[Command] = Behaviors.setup { ctx =>

    val voyages: ArrayBuffer[Voyage] = ArrayBuffer()
    val voyage: ArrayBuffer[AISPosition] = ArrayBuffer()
    var voyageId: String = s"$id-${UUID.randomUUID().toString}"

    var previousPosition = AISPosition(-1, -1, 0.0, 0.0, 0.0, 0.0, 0.0, 0L, "", "", "", "", "")
    var previousWaypoint: Int = -1

    def beginNewVoyage(waypoint: Int): Unit = {
      val itinerary = s"${previousWaypoint}_to_$waypoint"
      val completedVoyage = geotools.Voyage(voyageId, itinerary, voyage)
      voyages.append(completedVoyage)
      voyage.clear() // begin new voyage
      voyageId = s"$id-${UUID.randomUUID().toString}" // new distinct voyage identifier
      previousWaypoint = waypoint // store the starting waypoint of the new voyage
    }

    def splitVoyage(pos: AISPosition, duration: Int): Unit = {
      if (pos.seconds - previousPosition.seconds >= duration && previousPosition.nonEmpty()) {
        beginNewVoyage(-1)
      }
    }

    Behaviors.receiveMessage { cmd =>
      cmd match {
        case aisMessage(pos) =>
          LocalDatabase.getEnclosingWaypoint(GeoPoint(pos.longitude, pos.latitude)) match {
            case Some(waypoint) =>
              if (previousWaypoint != waypoint._2) { // different waypoint id which means that the voyage ended
                voyage.append(pos)
                beginNewVoyage(waypoint._2)
              }
              else {
                splitVoyage(pos,86400) // equal to or more than a day
                voyage.append(pos)
              }
            case None =>
              splitVoyage(pos,86400) // equal to or more than a day
              voyage.append(pos) // no waypoint is found, which means that the vessel is still travelling at the open sea and continues its voyage
          }
          previousPosition = pos
        case End =>
          val filteredVoyages = voyages.filter(v => !v.itinerary.contains("-1"))
          println("Ended.")
      }
      Behaviors.same
    }
  }

}
