package gr.hua.glasseas

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

class BalancedPartitioner(override val numPartitions: Int, nonDistinctKeys: RDD[Any]) extends Partitioner {

  // count number of positions per key
  var distribution = nonDistinctKeys.map(key => (key,1)).reduceByKey(_+_).collect().toMap
  // initialize each partition with zero elements
  var numKeysPerPartition: Map[Int,Int] = Map()
  (0 until numPartitions).foreach(k => numKeysPerPartition += (k -> 0))

  override def getPartition(key: Any): Int = key match {
    case id: Int =>
      getLeastLoadedPartition(id)
    case sid: String =>
      distribution = distribution.map(x => (x._1.hashCode(),x._2))
      val id = sid.hashCode
      getLeastLoadedPartition(id)
  }

  private def getLeastLoadedPartition(id: Int): Int = {
    // get least loaded partition
    val partitionId = numKeysPerPartition.minBy(_._2)._1
    // update the load of the partition
    val counts = distribution(id)
    val sum = numKeysPerPartition(partitionId) + counts
    numKeysPerPartition += (partitionId -> sum)
    partitionId
  }


}
