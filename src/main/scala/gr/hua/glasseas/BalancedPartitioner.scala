package gr.hua.glasseas

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

class BalancedPartitioner(override val numPartitions: Int, nonDistinctKeys: RDD[Int]) extends Partitioner {

  // count number of positions per mmsi
  val distribution = nonDistinctKeys.map(key => (key,1)).reduceByKey(_+_).collect().toMap
  // initialize each partition with zero elements
  var numKeysPerPartition: Map[Int,Int] = Map()
  (0 until numPartitions).foreach(k => numKeysPerPartition += (k -> 0))

  override def getPartition(key: Any): Int = key match {
    case id: Int =>
      val partitionId = numKeysPerPartition.minBy(_._2)._1
      val counts = distribution(id)
      val sum = numKeysPerPartition(partitionId) + counts
      numKeysPerPartition += (partitionId -> sum)
      partitionId
  }


}
