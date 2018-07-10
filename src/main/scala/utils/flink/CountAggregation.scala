package utils.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import utils.Parser

/**
  * The accumulator is used to keep a running sum and a count.
  * One counter is initialized for each hour of the day
  */
class CountAggregation extends AggregateFunction[Long, Array[Int], Array[Int]] {

  override def createAccumulator(): Array[Int] = {
    val array = new Array[Int](24)
    array
  }

  override def add(value: Long, accumulator: Array[Int]): Array[Int] = {
    val hour: Int = Parser.convertToDateTime(value).hourOfDay.get()
    accumulator(hour) += 1
    accumulator
  }

  override def getResult(accumulator: Array[Int]): Array[Int] = accumulator

  override def merge(a: Array[Int], b: Array[Int]): Array[Int] = {
    a.zip(b).map { case (x, y) => x + y }
  }

}

class CountDaily extends AggregateFunction[(Long, String), Array[Int], Array[Int]] {
  override def createAccumulator(): Array[Int] = new Array[Int](24)

  override def add(value: (Long, String), accumulator: Array[Int]): Array[Int] = {
    val hour = Parser.convertToDateTime(value._1).hourOfDay().get()
    accumulator(hour) += 1
    accumulator
  }

  override def getResult(accumulator: Array[Int]): Array[Int] = accumulator

  override def merge(a: Array[Int], b: Array[Int]): Array[Int] = a.zip(b).map { case (x, y) => x + y }

}


class CountWeekly extends AggregateFunction[(String, Long, Array[Int]), Array[Int], Array[Int]] {
  override def createAccumulator(): Array[Int] = new Array[Int](24)

  override def add(value: (String, Long, Array[Int]), accumulator: Array[Int]): Array[Int] = {
    accumulator.zip(value._3).map { case (x, y) => x + y }
  }

  override def getResult(accumulator: Array[Int]): Array[Int] = accumulator

  override def merge(a: Array[Int], b: Array[Int]): Array[Int] = a.zip(b).map { case (x, y) => x + y }

}

class CountDailyWithLatency extends AggregateFunction[(Long, String, Long), Array[Int], (Array[Int], Long)] {
  var timestamp: Long = 0

  override def createAccumulator(): Array[Int] = new Array[Int](24)

  override def add(value: (Long, String, Long), accumulator: Array[Int]): Array[Int] = {
    val hour = Parser.convertToDateTime(value._1).hourOfDay().get()
    accumulator(hour) += 1

    if (value._3 < timestamp || timestamp == 0)
      timestamp = value._3

    accumulator

  }

  override def getResult(accumulator: Array[Int]): (Array[Int], Long) = (accumulator, timestamp)

  override def merge(a: Array[Int], b: Array[Int]): Array[Int] = a.zip(b).map { case (x, y) => x + y }

}

class CountWeeklyWithLatency extends AggregateFunction[(String, Long, Array[Int], Long), Array[Int], (Array[Int], Long)] {
  var timestamp = 0L

  override def createAccumulator(): Array[Int] = new Array[Int](24)

  override def add(value: (String, Long, Array[Int], Long), accumulator: Array[Int]): Array[Int] = {
    accumulator.zip(value._3).map { case (x, y) => x + y }

    if (value._4 < timestamp || timestamp == 0)
      timestamp = value._4

    accumulator

  }

  override def getResult(accumulator: Array[Int]): (Array[Int], Long) = (accumulator, timestamp)

  override def merge(a: Array[Int], b: Array[Int]): Array[Int] = a.zip(b).map { case (x, y) => x + y }

}

class ComputeLatency extends ProcessFunction[(Long, Array[Int], Long), Float] {

  var count = 0F
  var sum = 0F

  override def processElement(value: (Long, Array[Int], Long), ctx: ProcessFunction[(Long, Array[Int], Long), Float]#Context, out: Collector[Float]): Unit = {
    count += 1
    sum += (System.currentTimeMillis() - value._3)

    out.collect(sum / count)
  }
}
