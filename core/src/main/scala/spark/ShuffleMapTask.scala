package spark

import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.util.{HashMap => JHashMap}

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

class ShuffleMapTask(
    runId: Int,
    stageId: Int,
    rdd: RDD[_], 
    dep: ShuffleDependency[_,_,_],
    val partition: Int, 
    locs: Seq[String])
  extends DAGTask[String](runId, stageId)
  with Logging {
  
  val split = rdd.splits(partition)

  override def run (attemptId: Int): String = {
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]

    val ser = SparkEnv.get.serializer.newInstance()
    val outs = new Array[SerializationStream](numOutputSplits)
    for (i <- 0 until numOutputSplits) {
      val file = SparkEnv.get.shuffleManager.getOutputFile(dep.shuffleId, partition, i)
      outs(i) = ser.outputStream(new FastBufferedOutputStream(new FileOutputStream(file)))
    }
    
    for (elem <- rdd.iterator(split)) {
      val pair = elem.asInstanceOf[(Any, Any)]
      var bucketId = partitioner.getPartition(pair._1)
      outs(bucketId).writeObject(pair)
    }

    outs.foreach { _.close() }
    return SparkEnv.get.shuffleManager.getServerUri
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
