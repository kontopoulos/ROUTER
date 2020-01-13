package gr.hua.glasseas

import java.text.SimpleDateFormat
import java.util.Date

import gr.hua.glasseas.geotools.{AISPosition, GeoPoint, SpatialToolkit, Voyage}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object InterpolationSparkApp {

  def main(args: Array[String]): Unit = {

    val shipType = "Cargo"

    LocalDatabase.initialize(s"waypoints/dataset_${shipType}_waypoints_2000.0_10.csv")

    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val filename = s"dataset_${shipType}_voyages.csv"

    val gc = new GlasseasContext
    val data = gc.readVoyageData(filename, sc)

    data.groupBy(_._2.annotation).map {
      case (voyageId, positionsWithItinerary) =>
        val itinerary = positionsWithItinerary.head._1

        val st = new SpatialToolkit
        val timeIncrement = 180
        val interpolated: ArrayBuffer[AISPosition] = ArrayBuffer()

        positionsWithItinerary.map(_._2).groupBy(p => (p.longitude, p.latitude)).filter(_._2.size == 1).values.flatten.toList.sortBy(_.seconds).sliding(3, 1).foreach {
          case positions =>
            if (positions.size == 3) {
              val second = positions(1)
              val third = positions.last

              val x = positions.map(_.longitude).toArray
              val y = positions.map(_.latitude).toArray

              val timeDiff = Math.abs(second.seconds - third.seconds)

              val numIncrements = timeDiff / timeIncrement - 1 // get the number of interpolated positions

              // calculate the difference of longitudes taking into account negative values
              val longitudeDiff =
                if (second.longitude < 0 && third.longitude < 0) Math.abs(Math.abs(second.longitude) - Math.abs(third.longitude))
                else if (second.longitude > 0 && third.longitude > 0) Math.abs(second.longitude - third.longitude)
                else Math.abs(second.longitude) + Math.abs(third.longitude)
              // get the longitude step for each interpolating position
              val longitudeIncrement = longitudeDiff / numIncrements

              if (timeDiff > timeIncrement * 2 && longitudeDiff != 0.0) {
                var currentIncrement = 1
                var currentLongitude = second.longitude + longitudeIncrement
                var currentSeconds = second.seconds + timeIncrement
                var previousLongitude = second.longitude
                var previousLatitude = second.latitude
                while (currentIncrement != numIncrements) { // loop while we reach the number of total increments
                  val interpolatedLatitude = st.lagrangeInterpolation(x, y, currentLongitude) // interpolate
                  val distance = st.getHaversineDistance(GeoPoint(previousLongitude, previousLatitude), GeoPoint(currentLongitude, interpolatedLatitude))
                  val currentSpeed = distance / 0.05 / 1.852 // calculate speed (km/h) of interpolated position and convert to knots

                  val date = new Date(currentSeconds * 1000)
                  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                  val formattedDate = sdf.format(date)

                  // calculate the bearing of the interpolated position
                  val dl = if (previousLongitude < 0 && currentLongitude < 0) Math.abs(Math.abs(previousLongitude) - Math.abs(currentLongitude))
                  else if (previousLongitude > 0 && currentLongitude > 0) Math.abs(previousLongitude - currentLongitude)
                  else Math.abs(previousLongitude) + Math.abs(currentLongitude)
                  val X = Math.cos(interpolatedLatitude.toRadians) * Math.sin(dl.toRadians)
                  val Y = Math.cos(previousLatitude.toRadians) * Math.sin(interpolatedLatitude.toRadians) - Math.sin(previousLatitude.toRadians) * Math.cos(interpolatedLatitude.toRadians) * Math.cos(dl.toRadians)
                  val bearing = Math.atan2(X, Y).toDegrees.toInt.toDouble

                  if (LocalDatabase.getEnclosingWaypoint(GeoPoint(currentLongitude, interpolatedLatitude)).isEmpty) interpolated.append(AISPosition(second.id, second.imo, interpolatedLatitude, currentLongitude, bearing, bearing, (currentSpeed * 10).toInt / 10.0, currentSeconds, formattedDate, second.shipname, second.shipType, second.destination, second.annotation))
                  previousLongitude = currentLongitude
                  previousLatitude = interpolatedLatitude
                  currentLongitude += longitudeIncrement
                  currentSeconds += timeIncrement
                  currentIncrement += 1
                }
              }
            }
        }

        val voyagePositions = positionsWithItinerary.map(_._2).to[ArrayBuffer]
        interpolated.foreach(ipos => voyagePositions.append(ipos))
        Voyage(voyageId, itinerary, voyagePositions.sortBy(_.seconds))
    }.saveAsTextFile(s"$filename-interpolated")
  }

}
