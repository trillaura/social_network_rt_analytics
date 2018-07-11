package utils.metrics

import org.joda.time.DateTime


class LatencyMeter extends Serializable {

  var count : Long = 0
  //private var mean : Double = 0
  private var sum : Long = 0

  def addLatencyFromTimestamp(timestamp: Long): Unit = {
    val now = System.nanoTime()
    val latency = now - timestamp
    count += 1
    sum += latency
    //mean =  mean + (latency - mean) / count
  }

  def shouldOutputLatency(): Boolean = {
    if(count % 10000 == 0){ return true }
    false
  }

  def printLatency(label: String): Unit ={
    println(label + " average latency " + averageLatency() + " ms")
  }

  def averageLatency(): Double = {
   // mean / (1000 * 1000)
    sum / count / 1000 / 1000
  }


}
