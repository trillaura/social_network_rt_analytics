package utils.flink

import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AddAllWindowStart extends ProcessAllWindowFunction[Array[Int], (Long, Array[Int]), TimeWindow] {


  override def process(context: Context, elements: Iterable[Array[Int]], out: Collector[(Long, Array[Int])]): Unit = {
    for (elem <- elements) {
      out.collect(context.window.getStart, elem)
    }
  }
}

class AddWindowStartDaily extends ProcessWindowFunction[Array[Int], (String, Long, Array[Int]), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[Array[Int]], out: Collector[(String, Long, Array[Int])]): Unit = {
    for (elem <- elements)
      out.collect(key, context.window.getStart, elem)
  }
}

class AddWindowStartDailyWithLatency extends ProcessWindowFunction[(Array[Int], Long), (String, Long, Array[Int], Long), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[(Array[Int], Long)], out: Collector[(String, Long, Array[Int], Long)]): Unit = {
    for (elem <- elements)
      out.collect(key, context.window.getStart, elem._1, elem._2)
  }
}

class AddWindowStartWeekly extends ProcessWindowFunction[Array[Int], (Long, Array[Int]), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[Array[Int]], out: Collector[(Long, Array[Int])]): Unit = {
    for (elem <- elements)
      out.collect(context.window.getStart, elem)
  }
}

class AddWindowStartWeeklyWithLatency extends ProcessWindowFunction[(Array[Int], Long), (Long, Array[Int], Long), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[(Array[Int], Long)], out: Collector[(Long, Array[Int], Long)]): Unit = {
    for (elem <- elements)
      out.collect(context.window.getStart, elem._1, elem._2)
  }
}