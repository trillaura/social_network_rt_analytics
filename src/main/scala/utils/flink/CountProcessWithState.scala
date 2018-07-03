package utils.flink

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


class CountProcessWithState extends ProcessAllWindowFunction[(Long, Array[Int]), (Long, Array[Int]), TimeWindow] with CheckpointedFunction {

  @transient
  private var checkpointedState: MapState[Int, Int] = _

  private val bufferedElements: Array[Int] = new Array[Int](24)


  override def process(context: Context, elements: Iterable[(Long, Array[Int])], out: Collector[(Long, Array[Int])]): Unit = {

    for (elem <- elements) {
      for (i <- elem._2.indices) {
        bufferedElements(i) += elem._2(i)
      }
    }
    out.collect((context.window.getStart, bufferedElements))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    updateState(bufferedElements)
  }

  def updateState(data: Array[Int]): Unit = {
    for (i <- data.indices) {
      val old = checkpointedState.get(i)
      val newVal = old + data(i)
      checkpointedState.put(i, newVal)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new MapStateDescriptor[Int, Int]("buffered-elements", classOf[Int], classOf[Int])
    checkpointedState = getRuntimeContext.getMapState(descriptor)

    if (context.isRestored) {
      val iterator = checkpointedState.keys().iterator()
      while (iterator.hasNext) {
        val key = iterator.next()
        bufferedElements(key) = checkpointedState.get(key)
      }
    }
  }

}