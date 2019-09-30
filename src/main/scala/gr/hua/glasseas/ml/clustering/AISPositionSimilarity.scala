package gr.hua.glasseas.ml.clustering

import de.lmu.ifi.dbs.elki.data.NumberVector
import de.lmu.ifi.dbs.elki.distance.distancefunction.AbstractNumberVectorDistanceFunction
import gr.hua.glasseas.geotools.{GeoPoint, SpatialToolkit}

class AISPositionSimilarity extends AbstractNumberVectorDistanceFunction {
  override def distance(o1: NumberVector, o2: NumberVector): Double = {
    val maxSpeedDifference = 50.0
    val maxHeadingDifference = 180.0

    val longitude1 = o1.doubleValue(0)
    val latitude1 = o1.doubleValue(1)
    val speed1 = o1.doubleValue(2)
    val heading1 = o1.doubleValue(3)
    val longitude2 = o2.doubleValue(0)
    val latitude2 = o2.doubleValue(1)
    val speed2 = o2.doubleValue(2)
    val heading2 = o2.doubleValue(3)
    val st = new SpatialToolkit
    val distance = st.getHarvesineDistance(GeoPoint(longitude1,latitude1),GeoPoint(longitude2,latitude2))
    val speedDifference = Math.abs(speed1-speed2)
    val headingDifference = if (Math.abs(heading1 - heading2) <= 180.0) Math.abs(heading1 - heading2) else Math.abs(360.0 - Math.max(heading1,heading2) + Math.min(heading1,heading2))
    /*if (distance <= 5.0) {
      val speedDifference = Math.abs(speed1-speed2)
      val headingDifference = if (Math.abs(heading1 - heading2) <= 180.0) Math.abs(heading1 - heading2) else Math.abs(360.0 - Math.max(heading1,heading2) + Math.min(heading1,heading2))
      speedDifference/maxSpeedDifference*0.5 + headingDifference/maxHeadingDifference*0.5
    }
    else 1.0*/
    if (distance <= 10.0 && speedDifference <= 3.0 && headingDifference <= 3.0) 0.0
    else 1.0
  }
}
