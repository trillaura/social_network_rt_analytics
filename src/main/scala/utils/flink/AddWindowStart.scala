package utils.flink

import org.apache.flink.api.common.state.ListState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AddAllWindowStart extends ProcessAllWindowFunction[Array[Int], (Long, Array[Int]), TimeWindow] {


  override def process(context: Context, elements: Iterable[Array[Int]], out: Collector[(Long, Array[Int])]): Unit = {
    for (elem <- elements)
      out.collect(context.window.getStart, elem)
  }
}

class AddWindowStart extends ProcessWindowFunction[Array[Int], (Long, Array[Int]), Tuple1[Int], TimeWindow] {
  override def process(key: Tuple1[Int], context: Context, elements: Iterable[Array[Int]], out: Collector[(Long, Array[Int])]): Unit = {
    for (elem <- elements)
      out.collect(context.window.getStart, elem)
  }
}

class HourlyCount extends ProcessFunction[(Int, Int), (Long, Array[Int])] with CheckpointedFunction {

  var countState: ListState[Int] = _
  var counts: Array[Int] = new Array[Int](24)

  override def processElement(value: (Int, Int), ctx: ProcessFunction[(Int, Int), (Long, Array[Int])]#Context, out: Collector[(Long, Array[Int])]): Unit = {
    counts(value._1) = value._2
//    val startStatistics: Long =
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = ???

  override def initializeState(context: FunctionInitializationContext): Unit = ???
}