package gr.hua.glasseas.ml.clustering

import java.io.FileWriter
import java.util

import de.lmu.ifi.dbs.elki.algorithm.clustering.DBSCAN
import de.lmu.ifi.dbs.elki.data.NumberVector
import de.lmu.ifi.dbs.elki.data.`type`.TypeUtil
import de.lmu.ifi.dbs.elki.database.StaticArrayDatabase
import de.lmu.ifi.dbs.elki.datasource.ArrayAdapterDatabaseConnection
import de.lmu.ifi.dbs.elki.distance.distancefunction.geo.LngLatDistanceFunction
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.RTreeSettings
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.rstar.RStarTreeFactory
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.strategies.bulk.SortTileRecursiveBulkSplit
import de.lmu.ifi.dbs.elki.math.geodesy.WGS84SpheroidEarthModel
import de.lmu.ifi.dbs.elki.persistent.MemoryPageFileFactory

import scala.collection.JavaConverters._
import gr.hua.glasseas.geotools.{AISPosition, GeoPoint, SpatialToolkit}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @param inputValues dataset
  * @param eps should be in meters
  * @param s absolute difference of speed
  * @param h absolute difference of heading
  * @param minPts number of minimum points
  */
class DBScan(inputValues: ArrayBuffer[AISPosition], eps: Double, s: Double, h: Double, minPts: Int) {

  def this(inputValues: ArrayBuffer[AISPosition], eps: Double, minPts: Int) = {
    this(inputValues,eps,3.0,3.0,minPts)
  }

  /**
    * Modified DB-Scan implementation
    * @return clusters
    */
  /*def getTrajectoryClusters: ArrayBuffer[ArrayBuffer[AISPosition]] = {
    val results: ArrayBuffer[ArrayBuffer[AISPosition]] = ArrayBuffer()
    var visitedPoints: Set[AISPosition] = Set()

    var neighbors: ArrayBuffer[AISPosition] = ArrayBuffer()
    var index = 0
    while (inputValues.size > index) {
      val p = inputValues.apply(index)
      if (!visitedPoints.contains(p)) {
        visitedPoints += p
        neighbors = getTrajectoryNeighbors(p)
        if (neighbors.size >= minPts) {
          var idx = 0
          while (neighbors.size > idx) {
            val r = neighbors.apply(idx)
            if (!visitedPoints.contains(r)) {
              visitedPoints += r
              val individualNeighbors = getTrajectoryNeighbors(r)
              if (individualNeighbors.size >= minPts) {
                neighbors = mergeRightToLeftCollecttion(neighbors,individualNeighbors)
              }
            }
            idx += 1
          }
          results.append(neighbors)
        }
      }
      index += 1
    }
    println(s"Number of clusters found: ${results.size}")
    results
  }*/

  /*private def getTrajectoryNeighbors(p: AISPosition): ArrayBuffer[AISPosition] = {

    val st = new SpatialToolkit
    val neighbors: ArrayBuffer[AISPosition] = ArrayBuffer()
    inputValues.foreach{
      candidate =>
        val speedDiff = Math.abs(candidate.speed-p.speed)
        val spatialDistance = st.getHarvesineDistance(GeoPoint(candidate.longitude,candidate.latitude),GeoPoint(p.longitude,p.latitude))

        if (speedDiff <= s && st.isTurningPoint(candidate,p,h) && spatialDistance <= eps) neighbors.append(candidate)
    }
    neighbors
  }

  private def getNeighbors(p: AISPosition): ArrayBuffer[AISPosition] = {

    val st = new SpatialToolkit
    val neighbors: ArrayBuffer[AISPosition] = ArrayBuffer()
    inputValues.foreach{
      candidate =>
        val spatialDistance = st.getHarvesineDistance(GeoPoint(candidate.longitude,candidate.latitude),GeoPoint(p.longitude,p.latitude))

        if (spatialDistance <= eps) neighbors.append(candidate)
    }
    neighbors
  }

  private def mergeRightToLeftCollecttion(neighbors1: ArrayBuffer[AISPosition], neighbors2: ArrayBuffer[AISPosition]): ArrayBuffer[AISPosition] = {
    neighbors2.foreach{
      tempPt =>
        if (!neighbors1.contains(tempPt)) {
          neighbors1.append(tempPt)
        }
    }
    neighbors1
  }*/

  /**
    * Typical DBScan implementation
    * @return
    */
  def getClusters: ArrayBuffer[ArrayBuffer[AISPosition]] = {
    val results: ArrayBuffer[ArrayBuffer[AISPosition]] = ArrayBuffer()

    val numPoints = inputValues.length
    var correlationMap: Map[GeoPoint,ArrayBuffer[AISPosition]] = Map()

    val data = Array.ofDim[Double](numPoints, 2)
    for (i <- 0 until numPoints) {
      data(i)(0) = inputValues(i).longitude
      data(i)(1) = inputValues(i).latitude
      // keep initial positions for future reference
      correlationMap.get(GeoPoint(inputValues(i).longitude,inputValues(i).latitude)) match {
        case Some(arr) =>
          val newArr = arr
          arr.append(inputValues(i))
          correlationMap += (GeoPoint(inputValues(i).longitude,inputValues(i).latitude) -> newArr)
        case None => correlationMap += (GeoPoint(inputValues(i).longitude,inputValues(i).latitude) -> ArrayBuffer(inputValues(i)))
      }
    }

    val db = new StaticArrayDatabase(new ArrayAdapterDatabaseConnection(data, null, 0), util.Arrays.asList(new RStarTreeFactory(new MemoryPageFileFactory(4096), new RTreeSettings(SortTileRecursiveBulkSplit.STATIC))))
    db.initialize()

    val rel = db.getRelation[NumberVector](TypeUtil.NUMBER_VECTOR_FIELD)

    val clusters = new DBSCAN(new LngLatDistanceFunction(WGS84SpheroidEarthModel.STATIC), eps, minPts).run(db).getAllClusters.asScala

    for (cluster <- clusters) {
      val clusterGroup: ArrayBuffer[AISPosition] = ArrayBuffer()
      if (!cluster.isNoise) {
        val it = cluster.getIDs.iter()
        while (it.valid()) {
          val p = rel.get(it)
          val lon = p.doubleValue(0)
          val lat = p.doubleValue(1)
          correlationMap(GeoPoint(lon,lat)).foreach(pos => clusterGroup.append(pos))
          it.advance()
        }
        results.append(clusterGroup)
      }
    }
    //println(s"Number of clusters found: ${results.size}")
    results
  }

  /**
    * Modified DBScan implementation.
    * Recommended parameters for sampled trajectories:
    * eps: 0.03
    * minPts: 10
    * @return
    */
  def getTrajectoryClusters: ArrayBuffer[ArrayBuffer[AISPosition]] = {
    val results: ArrayBuffer[ArrayBuffer[AISPosition]] = ArrayBuffer()

    val numPoints = inputValues.length
    var correlationMap: Map[GeoPoint,ArrayBuffer[AISPosition]] = Map()

    val data = Array.ofDim[Double](numPoints, 4)
    for (i <- 0 until numPoints) {
      data(i)(0) = inputValues(i).longitude
      data(i)(1) = inputValues(i).latitude
      data(i)(2) = inputValues(i).speed
      data(i)(3) = inputValues(i).cog
      // keep initial positions for future reference
      correlationMap.get(GeoPoint(inputValues(i).longitude,inputValues(i).latitude)) match {
        case Some(arr) =>
          val newArr = arr
          arr.append(inputValues(i))
          correlationMap += (GeoPoint(inputValues(i).longitude,inputValues(i).latitude) -> newArr)
        case None => correlationMap += (GeoPoint(inputValues(i).longitude,inputValues(i).latitude) -> ArrayBuffer(inputValues(i)))
      }
    }

    val db = new StaticArrayDatabase(new ArrayAdapterDatabaseConnection(data, null, 0), util.Arrays.asList(new RStarTreeFactory(new MemoryPageFileFactory(4096), new RTreeSettings(SortTileRecursiveBulkSplit.STATIC))))
    db.initialize()

    val rel = db.getRelation[NumberVector](TypeUtil.NUMBER_VECTOR_FIELD)

    val clusters = new DBSCAN(new AISPositionSimilarity, eps, minPts).run(db).getAllClusters.asScala

    for (cluster <- clusters) {
      val clusterGroup: ArrayBuffer[AISPosition] = ArrayBuffer()
      if (!cluster.isNoise) {
        val it = cluster.getIDs.iter()
        while (it.valid()) {
          val p = rel.get(it)
          val lon = p.doubleValue(0)
          val lat = p.doubleValue(1)
          correlationMap(GeoPoint(lon,lat)).foreach(pos => clusterGroup.append(pos))
          it.advance()
        }
        results.append(clusterGroup)
      }
    }
    //println(s"Number of clusters found: ${results.size}")
    results
  }

}
