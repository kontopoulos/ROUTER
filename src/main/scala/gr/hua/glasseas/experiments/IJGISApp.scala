package gr.hua.glasseas.experiments

import gr.hua.glasseas.{GlasseasContext, LocalDatabase}
import gr.hua.glasseas.geotools.GeoPoint
import org.apache.spark.{SparkConf, SparkContext}

object IJGISApp {

  def main(args: Array[String]): Unit = {
    /* ============= CREATE 10 FOLDS ============= */

    /*val shipType = "Passenger"
    val filename = s"dataset_${shipType}_voyages.csv"
//    val filename = "dataset_Passenger_voyages.csv"
//    val filename = "dataset_Tanker_voyages.csv"
    val numFolds = 10

    val itineraries = scala.io.Source.fromFile(filename).getLines().drop(1).map(_.split(",")(1)).toList.distinct
    val shuffled = Random.shuffle(itineraries)

    val foldWriter = new FileWriter(s"${shipType}_${numFolds}_folds.csv")
    foldWriter.write("FOLD,TYPE,ITINERARY\n")
    for (fold <- 0 until numFolds) {
      println(s"Current fold: $fold")
      val trainset = shuffled.slice(0, fold) ++ shuffled.slice(fold+shuffled.length/numFolds, shuffled.length)
      val testset = shuffled.slice(fold, fold+shuffled.length/numFolds)

      trainset.foreach(itinerary => foldWriter.write(s"$fold,TRAIN,$itinerary\n"))
      testset.foreach(itinerary => foldWriter.write(s"$fold,TEST,$itinerary\n"))
    }
    foldWriter.close()*/

    /* ============= COUNT VESSELS - VOYAGES ============= */

    /*var vessels: Set[String] = Set()
    var voyages: Set[String] = Set()

    scala.io.Source.fromFile("dataset_Cargo_voyages.csv").getLines().drop(1).foreach{
      line =>
        val columns = line.split(",")
        val shipId = columns(2)
        val voyageId = columns.head
        vessels += shipId
        voyages += voyageId
    }

    println(s"Vessels: ${vessels.size}")
    println(s"Voyages: ${voyages.size}")*/

    /* ============= COUNT ACCURACY ============= */

    /*val shipType = "Passenger"
    val folds = scala.io.Source.fromFile(s"${shipType}_10_folds.csv").getLines().drop(1).map{
      line =>
        val columns = line.split(",")
        val fold = columns.head.toInt
        val fType = columns(1)
        val itinerary = columns.last
        (fold,fType,itinerary)
    }.toArray.groupBy(_._1).mapValues(_.map(v => (v._2,v._3)))


    LocalDatabase.initialize(s"waypoints/dataset_${shipType}_waypoints_2000.0_10.csv")

    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val w = new FileWriter(s"${shipType}_accuracies.csv")
    w.write("FOLD,ACCURACY,STD\n")

    val accuracies: ArrayBuffer[Double] = ArrayBuffer()
    for (fold <- 0 until 10) {

      val properties = folds(fold)
      val trainItineraries = properties.filter(_._1 == "TRAIN").map(_._2).toSet
      val testItineraries = properties.filter(_._1 == "TEST").map(_._2).toSet

      LocalDatabase.emptyHulls
      LocalDatabase.readPartialConvexHulls(s"dataset_${shipType}_convexhulls.csv",trainItineraries)

      val gc = new GlasseasContext
      val data = gc.readVoyageData(s"dataset_${shipType}_voyages.csv",sc)
        .filter(p => LocalDatabase.getEnclosingWaypoint(GeoPoint(p._2.longitude,p._2.latitude)).isEmpty) // remove positions located inside the waypoints
        .filter(it => testItineraries.contains(it._1)) // get itineraries that are contained in the test dataset
      val totalPositions = data.count().toDouble

      val inHulls = data.filter{
        case (itinerary,p) =>
          LocalDatabase.getEnclosingConvexHull(GeoPoint(p.longitude,p.latitude)) match {
            case Some(hull) => true
            case None => false
          }
      }.count().toDouble

      val accuracy = inHulls/totalPositions
      accuracies.append(accuracy)
      println(s"Fold: $fold\nAccuracy: $accuracy\ninHulls: $inHulls\ntotalPositions: $totalPositions")
      w.write(s"$fold,$accuracy,-1\n")
    }
    val meanAccuracy = accuracies.sum/accuracies.size
    val stdAccuracy = Math.sqrt(accuracies.map(a => Math.pow(Math.abs(a - meanAccuracy), 2)).sum / accuracies.size)
    println(s"Avg accuracy: $meanAccuracy\nStd: $stdAccuracy")
    w.write(s"-1,$meanAccuracy,$stdAccuracy\n")
    w.close()*/

    /* ============= CELLS DEVIATIONS ============= */

    val shipType = "Cargo"
    LocalDatabase.initialize(s"waypoints/dataset_${shipType}_waypoints_2000.0_10.csv")

    val conf = new SparkConf().setAppName("GLASSEAS").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val filename = s"dataset_${shipType}_voyages.csv"

    val gc = new GlasseasContext
    gc.readVoyageData(filename,sc).filter(p => LocalDatabase.getEnclosingWaypoint(GeoPoint(p._2.longitude,p._2.latitude)).isEmpty).map{
      case (itinerary,position) =>
        val cellId = LocalDatabase.grid.getEnclosingCell(GeoPoint(position.longitude,position.latitude)).get.id
        (itinerary,cellId,position)
    }.groupBy(x => (x._1,x._2)).map{
      case (id,arr) =>
        val itinerary = id._1
        val cell = id._2
        val positions = arr.map(_._3)
        val meanHeading = positions.map(_.cog).sum / positions.size
        val stdHeading = Math.sqrt(positions.map(p => Math.pow(Math.abs(p.cog - meanHeading), 2)).sum / positions.size)
        val minHeading = positions.minBy(_.cog).cog
        val maxHeading = positions.maxBy(_.cog).cog

        val meanSpeed = positions.map(_.speed).sum / positions.size
        val stdSpeed = Math.sqrt(positions.map(p => Math.pow(Math.abs(p.speed - meanSpeed), 2)).sum / positions.size)
        val minSpeed = positions.minBy(_.speed).speed
        val maxSpeed = positions.maxBy(_.speed).speed
        s"${itinerary}-${cell},$meanHeading,$stdHeading,$minHeading,$maxHeading,$meanSpeed,$stdSpeed,$minSpeed,$maxSpeed"
    }.saveAsTextFile(s"$filename-icells")
  }

}
