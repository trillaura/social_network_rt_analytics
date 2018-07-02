package utils.flink

import model.UserConnection
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * The accumulator is used to keep a running sum and a count.
  * One counter is initialized for each hour of the day
  */
class CountAggregation extends AggregateFunction[(UserConnection, Int), Array[Int], Array[Int]] {

  var array: Array[Int] = _

  override def createAccumulator(): Array[Int] = {
    array = new Array[Int](24)
    array
  }

  override def add(value: (UserConnection, Int), accumulator: Array[Int]): Array[Int] = {
    accumulator(value._2) += 1
    accumulator
  }

  override def getResult(accumulator: Array[Int]): Array[Int] = accumulator

  override def merge(a: Array[Int], b: Array[Int]): Array[Int] = {
    a.zip(b).map { case (x, y) => x + y }
  }

}
