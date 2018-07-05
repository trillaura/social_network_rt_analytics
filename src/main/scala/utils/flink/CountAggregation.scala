package utils.flink

import org.apache.flink.api.common.functions.AggregateFunction
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