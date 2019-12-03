package gr.hua.glasseas.geotools

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class SpatialToolkit extends Serializable {

  /**
    * Checks the difference in headings between two AIS positions
    * @param first AIS position
    * @param second AIS position
    * @param threshold difference in heading threshold
    * @return
    */
  def isTurningPoint(first: AISPosition, second: AISPosition, threshold: Double): Boolean = {
    val angleDiff = Math.abs(first.cog - second.cog)
    if (angleDiff <= 180.0) {
      if (angleDiff >= threshold) true else false
    }
    else {
      val diff = Math.abs(360.0 - Math.max(first.cog,second.cog) + Math.min(first.cog,second.cog))
      if (diff >= threshold) true else false
    }
  }

  /**
    * Returns the difference in headings between two AIS positions
    * @param first AIS position
    * @param second AIS position
    * @return
    */
  def headingDifference(first: AISPosition, second: AISPosition): Double = {
    val angleDiff = Math.abs(first.cog - second.cog)
    if (angleDiff <= 180.0) angleDiff
    else Math.abs(360.0 - Math.max(first.cog,second.cog) + Math.min(first.cog,second.cog))
  }

  def getProjection(speed: Double, course: Double, longitude: Double, latitude: Double, currentTimePoint: Long, futureTimePoint: Long): GeoPoint = {
    val heading = course.toRadians
    val sog = speed*1.852 // convert to kmh
    if (sog != 0.0) {
      val lon = longitude.toRadians
      val lat = latitude.toRadians
      val delta = (futureTimePoint - currentTimePoint) * sog / 3600 / 6371
      val newLatitude: Double = BigDecimal(Math.asin((Math.sin(lat) * Math.cos(delta)) + (Math.cos(lat) * Math.sin(delta) *
        Math.cos(heading))).toDegrees).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
      val newLongitude: Double = BigDecimal((lon + Math.atan2(Math.sin(heading) * Math.sin(delta) * Math.cos(lat),
        Math.cos(delta) - (Math.sin(lat) * Math.sin(newLatitude.toRadians)))).toDegrees).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
      GeoPoint(newLongitude,newLatitude)
    }
    else GeoPoint(longitude,latitude)
  }

  /**
    * This uses the ‘haversine’ formula to calculate
    * the great-circle distance between two points – that is,
    * the shortest distance over the earth’s surface – giving an ‘as-the-crow-flies’ distance
    * between the points
    * (ignoring any hills they fly over, of course)
    * @param gp1 geo-point 1
    * @param gp2 geo-point 2
    * @return distance in kilometers
    */
  def getHaversineDistance(gp1: GeoPoint, gp2: GeoPoint): Double = {
    val lon1Rad = gp1.longitude*(Math.PI/180)
    val lat1Rad = gp1.latitude*(Math.PI/180)
    val lon2Rad = gp2.longitude*(Math.PI/180)
    val lat2Rad = gp2.latitude*(Math.PI/180)
    val dLon = lon2Rad - lon1Rad
    val dLat = lat2Rad - lat1Rad
    val a = Math.pow(Math.sin(dLat/2),2) + Math.cos(lat1Rad)*Math.cos(lat2Rad)*Math.pow(Math.sin(dLon/2),2)
    val c = 2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a))
    // earth radius = 3961 miles / 6371 km
    6371*c
  }

  /**
    * Calculates the Euclidean distance between two points.
    * It uses the UTM coordinates to calculate the distance.
    * Note that in large geographic distances, the euclidean distance does not make much sense.
    * @param gp1 geo-point 1
    * @param gp2 geo-point 2
    * @return distance in kilometers
    */
  def getEuclideanDistance(gp1: GeoPoint, gp2: GeoPoint): Double = {
    val cp1 = geodeticToCartesian(gp1)
    val cp2 = geodeticToCartesian(gp2)

    Math.sqrt(Math.pow(cp2.x-cp1.x,2.0)+Math.pow(cp2.y-cp1.y,2.0))/1000
  }

  def cartesianToGeodetic(p: CartesianPoint): GeoPoint = {
    /*val R = 6371
    val r = Math.sqrt(Math.pow(p.x,2) + Math.pow(p.y,2))
    val longitude = 180 * Math.atan2(p.y,p.x)/Math.PI
    val latitude = 180 * Math.acos(r/R)/Math.PI
    GeoPoint(longitude,latitude)*/

    var r = Math.sqrt(p.x*p.x + p.y*p.y+ p.z*p.z)
    var lat = Math.asin(p.z/r).toDegrees
    var lon = Math.atan2(p.y,p.x).toDegrees
    GeoPoint(lon,lat)

  }

  /**
    * Converts geographic coordinates to UTM coordinate system.
    * The Universal Transverse Mercator (UTM) conformal projection uses
    * a 2-dimensional Cartesian coordinate system to give locations on the surface of the Earth.
    * Like the traditional method of latitude and longitude, it is a horizontal position representation,
    * i.e. it is used to identify locations on the Earth independently of altitude.
    * @param p geo-point
    * @return cartesinan coordinates
    */
  def geodeticToCartesian(p: GeoPoint): CartesianPoint = {
    /*val R = 6371
    val x = R*Math.cos(p.latitude.toRadians)*Math.cos(p.longitude.toRadians)
    val y = R*Math.cos(p.latitude.toRadians)*Math.sin(p.longitude.toRadians)
    val z = R*Math.sin(p.latitude.toRadians)
    CartesianPoint(x,y,z)*/

    val R = 6371
    var lat = p.latitude.toRadians
    var lon = p.longitude.toRadians
    var x = R * Math.cos(lat)*Math.cos(lon)
    var y = R * Math.cos(lat)*Math.sin(lon)
    var z = R * Math.sin(lat)
    CartesianPoint(x,y,z)

    /*var easting = 0.0
    var northing = 0.0
    val zone = Math.floor(p.longitude/6+31).toInt
    var letter = ""

    if (p.latitude < -72) letter = "C"
    else if (p.latitude < -64) letter = "D"
    else if (p.latitude < -56) letter = "E"
    else if (p.latitude < -48) letter = "F"
    else if (p.latitude < -40) letter = "G"
    else if (p.latitude < -32) letter = "H"
    else if (p.latitude < -24) letter = "J"
    else if (p.latitude < -16) letter = "K"
    else if (p.latitude < -8) letter = "L"
    else if (p.latitude < 0) letter = "M"
    else if (p.latitude < 8) letter = "N"
    else if (p.latitude < 16) letter = "P"
    else if (p.latitude < 24) letter = "R"
    else if (p.latitude < 32) letter = "R"
    else if (p.latitude < 40) letter = "S"
    else if (p.latitude < 48) letter = "T"
    else if (p.latitude < 56) letter = "U"
    else if (p.latitude < 64) letter= "V"
    else if (p.latitude < 72) letter = "W"
    else letter = "X"

    easting = 0.5*Math.log((1+Math.cos(p.latitude*Math.PI/180)*Math.sin(p.longitude*Math.PI/180-(6*zone-183)*Math.PI/180))/(1-Math.cos(p.latitude*Math.PI/180)*Math.sin(p.longitude*Math.PI/180-(6*zone-183)*Math.PI/180)))*0.9996*6399593.62/Math.pow(1+Math.pow(0.0820944379, 2)*Math.pow(Math.cos(p.latitude*Math.PI/180), 2), 0.5)*(1+ Math.pow(0.0820944379,2)/2*Math.pow(0.5*Math.log((1+Math.cos(p.latitude*Math.PI/180)*Math.sin(p.longitude*Math.PI/180-(6*zone-183)*Math.PI/180))/(1-Math.cos(p.latitude*Math.PI/180)*Math.sin(p.longitude*Math.PI/180-(6*zone-183)*Math.PI/180))),2)*Math.pow(Math.cos(p.latitude*Math.PI/180),2)/3)+500000
    easting = Math.round(easting*100)*0.01
    northing = (Math.atan(Math.tan(p.latitude*Math.PI/180)/Math.cos(p.longitude*Math.PI/180-(6*zone -183)*Math.PI/180))-p.latitude*Math.PI/180)*0.9996*6399593.625/Math.sqrt(1+0.006739496742*Math.pow(Math.cos(p.latitude*Math.PI/180),2))*(1+0.006739496742/2*Math.pow(0.5*Math.log((1+Math.cos(p.latitude*Math.PI/180)*Math.sin(p.longitude*Math.PI/180-(6*zone -183)*Math.PI/180))/(1-Math.cos(p.latitude*Math.PI/180)*Math.sin(p.longitude*Math.PI/180-(6*zone -183)*Math.PI/180))),2)*Math.pow(Math.cos(p.latitude*Math.PI/180),2))+0.9996*6399593.625*(p.latitude*Math.PI/180-0.005054622556*(p.latitude*Math.PI/180+Math.sin(2*p.latitude*Math.PI/180)/2)+4.258201531e-05*(3*(p.latitude*Math.PI/180+Math.sin(2*p.latitude*Math.PI/180)/2)+Math.sin(2*p.latitude*Math.PI/180)*Math.pow(Math.cos(p.latitude*Math.PI/180),2))/4-1.674057895e-07*(5*(3*(p.latitude*Math.PI/180+Math.sin(2*p.latitude*Math.PI/180)/2)+Math.sin(2*p.latitude*Math.PI/180)*Math.pow(Math.cos(p.latitude*Math.PI/180),2))/4+Math.sin(2*p.latitude*Math.PI/180)*Math.pow(Math.cos(p.latitude*Math.PI/180),2)*Math.pow(Math.cos(p.latitude*Math.PI/180),2))/3)
    if (letter < "M") northing = northing + 10000000
    northing = Math.round(northing*100)*0.01

    CartesianPoint(easting,northing)*/
  }

  def lagrangeInterpolation(x: Array[Double], y: Array[Double], xPoint: Double): Double = {
    var sum = 0.0
    var product = 1.0

    // Peforming Arithmetic Operation
    for (i <- 0 until x.length) {
      for (j <- 0 until y.length) {
        if (j != i) {
          product *= (xPoint - x(j)) / (x(i) - x(j))
        }
      }
      sum += product * y(i)

      product = 1 // Must set to 1
    }
    sum
  }

  /**
    * Creates a convex hull from a list of geo-points
    * @param points list of geo-points
    * @return polygon of convex hull
    */
  def getConvexHull(points: List[GeoPoint]): Polygon = {
    val sortedPoints = points.sortBy(_.longitude)
    val upper = halfHull(sortedPoints)
    val lower = halfHull(sortedPoints.reverse)
    upper.remove(0)
    lower.remove(0)
    val polygonPoints = (upper.toList ++ lower.toList)
    new Polygon(polygonPoints ++ List(polygonPoints.head))
  }

  private def halfHull(points: List[GeoPoint]) = {
    val upper = new ListBuffer[GeoPoint]()
    for (p <- points) {
      while (upper.size >= 2 && leftTurn(p, upper.head, upper(1))) {
        upper.remove(0)
      }
      upper.prepend(p)
    }
    upper
  }

  private def leftTurn(p1: GeoPoint, p2: GeoPoint, p3: GeoPoint) = {
    val slope = (p2.longitude - p1.longitude) * (p3.latitude - p1.latitude) - (p2.latitude - p1.latitude) * (p3.longitude - p1.longitude)
    val collinear = math.abs(slope) <= 1e-9
    val leftTurn = slope < 0
    collinear || leftTurn
  }

  /**
    * Creates a concave hull from a list of geo-points and a k parameter
    * @param geoPoints list of geo-points
    * @param k paramter of the algorithm
    * @return
    */
  def getConcaveHull(geoPoints: List[GeoPoint], k: Int): Polygon = {
    // the resulting concave hull
    val concaveHull: ArrayBuffer[GeoPoint] = ArrayBuffer()

    // optional remove duplicates
    val distinctGeoPoints = geoPoints.distinct.to[ArrayBuffer]

    // k has to be greater than 3 to execute the algorithm
    var kk = Math.max(k, 3)

    // return Points if already Concave Hull
    if (distinctGeoPoints.size < 3) {
      return new Polygon(distinctGeoPoints.toList)
    }

    // make sure that k neighbors can be found
    kk = Math.min(kk, distinctGeoPoints.size - 1)

    // find first point and remove from point list
    val firstPoint = findMinYPoint(distinctGeoPoints)
    concaveHull.append(firstPoint)
    var currentPoint = firstPoint
    distinctGeoPoints -= firstPoint

    var previousAngle = 0.0
    var step = 2

    while ((currentPoint != firstPoint || step == 2) && distinctGeoPoints.nonEmpty) {
      // after 3 steps add first point to dataset, otherwise hull cannot be closed
      if (step == 5) {
        distinctGeoPoints.append(firstPoint)
      }

      // get k nearest neighbors of current point
      val kNearestPoints = kNearestNeighbors(distinctGeoPoints, currentPoint, kk)

      // sort points by angle clockwise
      val clockwisePoints = sortByAngle(kNearestPoints, currentPoint, previousAngle)

      // check if clockwise angle nearest neighbors are candidates for concave hull
      var its = true
      var i = -1
      while (its && i < clockwisePoints.size - 1) {
        i += 1

        var lastPoint = 0
        if (clockwisePoints.apply(i) == firstPoint) {
          lastPoint = 1
        }

        // check if possible new concave hull point intersects with others
        var j = 2
        its = false
        while (!its && j < concaveHull.size - lastPoint) {
          its = intersect(concaveHull.apply(step - 2), clockwisePoints.apply(i), concaveHull.apply(step - 2 - j), concaveHull.apply(step - 1 - j))
          j += 1
        }
      }

      // if there is no candidate increase k - try again
      if (its) {
        return getConcaveHull(geoPoints, k + 1)
      }

      // add candidate to concave hull and remove from dataset
      currentPoint = clockwisePoints.apply(i)
      concaveHull.append(currentPoint)
      distinctGeoPoints -= currentPoint

      // calculate last angle of the concave hull line
      previousAngle = calculateAngle(concaveHull.apply(step - 1), concaveHull.apply(step - 2))

      step += 1

    }

    // Check if all points are contained in the concave hull
    var insideCheck = true
    var i = distinctGeoPoints.size - 1

    while (insideCheck && i > 0) {
      val cHull = new Polygon(concaveHull.toList)
      insideCheck = cHull.isInside(distinctGeoPoints.apply(i))
      i -= 1
    }

    // if not all points inside -  try again
    if (!insideCheck) {
      getConcaveHull(geoPoints, k + 1)
    } else {
      new Polygon(concaveHull.toList)
    }

  }

  private def kNearestNeighbors(l: ArrayBuffer[GeoPoint], q: GeoPoint, k: Int): ArrayBuffer[GeoPoint] = {
    val nearestList: ArrayBuffer[(Double,GeoPoint)] = ArrayBuffer()
    l.foreach{
      o =>
        nearestList.append((getHaversineDistance(q,o),o))
    }

    val sortedNearestList = nearestList.sortBy(_._1).map(_._2)
    sortedNearestList
  }

  private def findMinYPoint(l: ArrayBuffer[GeoPoint]): GeoPoint = {
    l.minBy(_.latitude)
  }

  private def calculateAngle(o1: GeoPoint, o2: GeoPoint): Double = {
    Math.atan2(o2.latitude - o1.latitude, o2.longitude - o1.longitude)
  }

  private def angleDifference(a1: Double, a2: Double): Double = {
    // calculate angle difference in clockwise directions as radians
    if ((a1 > 0 && a2 >= 0) && a1 > a2) {
      Math.abs(a1 - a2)
    } else if ((a1 >= 0 && a2 > 0) && a1 < a2) {
      2 * Math.PI + a1 - a2
    } else if ((a1 < 0 && a2 <= 0) && a1 < a2) {
      2 * Math.PI + a1 + Math.abs(a2)
    } else if ((a1 <= 0 && a2 < 0) && a1 > a2) {
      Math.abs(a1 - a2)
    } else if (a1 <= 0 && 0 < a2) {
      2 * Math.PI + a1 - a2
    } else if (a1 >= 0 && 0 >= a2) {
      a1 + Math.abs(a2)
    } else {
      0.0
    }
  }

  private def sortByAngle(l: ArrayBuffer[GeoPoint],q: GeoPoint,a: Double): ArrayBuffer[GeoPoint] = {
    l.sortWith{
      case (o1,o2) =>
        val a1 = angleDifference(a,calculateAngle(q,o1))
        val a2 = angleDifference(a,calculateAngle(q,o2))
        if (a2 > a1) true
        else false
    }
  }

  private def intersect(l1p1: GeoPoint, l1p2: GeoPoint, l2p1: GeoPoint, l2p2: GeoPoint): Boolean = {
    // calculate part equations for line-line intersection
    val a1 = l1p2.latitude - l1p1.latitude
    val b1 = l1p1.longitude - l1p2.longitude
    val c1 = a1 * l1p1.longitude + b1 * l1p1.latitude
    val a2 = l2p2.latitude - l2p1.latitude
    val b2 = l2p1.longitude - l2p2.longitude
    val c2 = a2 * l2p1.longitude + b2 * l2p1.latitude
    // calculate the divisor
    val tmp = a1 * b2 - a2 * b1

    // calculate intersection point x coordinate
    val pX = (c1 * b2 - c2 * b1) / tmp

    // check if intersection x coordinate lies in line line segment
    if ((pX > l1p1.longitude && pX > l1p2.longitude) || (pX > l2p1.longitude && pX > l2p2.longitude)
      || (pX < l1p1.longitude && pX < l1p2.longitude) || (pX < l2p1.longitude && pX < l2p2.longitude)) {
      return false
    }

    // calculate intersection point y coordinate
    val pY = (a1 * c2 - a2 * c1) / tmp

    // check if intersection y coordinate lies in line line segment
    if ((pY > l1p1.latitude && pY > l1p2.latitude) || (pY > l2p1.latitude && pY > l2p2.latitude)
      || (pY < l1p1.latitude && pY < l1p2.latitude) || (pY < l2p1.latitude && pY < l2p2.latitude)) {
      return false
    }
    true
  }

}
