package utils

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * The accumulator is used to keep a running sum and a count. The [getResult] method
  * computes the average.
  */
class CountAggregation extends AggregateFunction[(Long, Int, Int), Array[Int], Array[Int]] {
  override def createAccumulator(): Array[Int] = {
    val array: Array[Int] = new Array[Int](24)
    array
  }

  override def add(value: (Long, Int, Int), accumulator: Array[Int]): Array[Int] = {
    accumulator(value._2) += 1
    accumulator
  }

  override def getResult(accumulator: Array[Int]): Array[Int] = accumulator

  override def merge(a: Array[Int], b: Array[Int]): Array[Int] = {
    a.zip(b).map { case (x, y) => x + y }
  }

}
