package utils.flink

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import utils.Parser


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

class CountProcessWithStateSliding extends ProcessAllWindowFunction[Long, (Long, Array[Int]), TimeWindow] with CheckpointedFunction {

  @transient
  private var checkpointedState: MapState[Int, Int] = _

  private val bufferedElements: Array[Int] = new Array[Int](24)


  override def process(context: Context, elements: Iterable[Long], out: Collector[(Long, Array[Int])]): Unit = {
    for (elem <- elements) {
      val hour: Int = Parser.convertToDateTime(elem).hourOfDay.get()
      bufferedElements(hour) += 1
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

class CountWithState extends ProcessAllWindowFunction[(Long, Array[Int]), (Long, Array[Int]), GlobalWindow] with CheckpointedFunction {

  @transient
  private var checkpointedState: MapState[Int, Int] = _

  private val bufferedElements: Array[Int] = new Array[Int](24)

  private var startState: ValueState[Long] = _
  private var start: Long = 0


  override def process(context: Context, elements: Iterable[(Long, Array[Int])], out: Collector[(Long, Array[Int])]): Unit = {

    for (elem <- elements) {

      if (start == 0 || start > elem._1)
        start = elem._1

      for (i <- elem._2.indices) {
        bufferedElements(i) += elem._2(i)
      }
    }

    out.collect((start, bufferedElements))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    updateState(bufferedElements)
    startState.update(start)
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

    startState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("start", classOf[Long]))

    if (context.isRestored) {
      val iterator = checkpointedState.keys().iterator()
      while (iterator.hasNext) {
        val key = iterator.next()
        bufferedElements(key) = checkpointedState.get(key)
      }
    }
  }

}

class CountWithStateAndLatency extends ProcessAllWindowFunction[(Long, Array[Int], Long), (Long, Array[Int], Float), GlobalWindow] with CheckpointedFunction {

  @transient
  private var checkpointedState: MapState[Int, Int] = _

  private val bufferedElements: Array[Int] = new Array[Int](24)

  private var startState: ValueState[Long] = _
  private var start: Long = 0

  private var count: Float = 0F
  private var sum: Float = 0F


  override def process(context: Context, elements: Iterable[(Long, Array[Int], Long)], out: Collector[(Long, Array[Int], Float)]): Unit = {

    for (elem <- elements) {

      if (start == 0 || start > elem._1)
        start = elem._1

      for (i <- elem._2.indices) {
        bufferedElements(i) += elem._2(i)
      }

      count += 1
      sum += (System.currentTimeMillis() - elem._3)
    }

    out.collect((start, bufferedElements, sum / count))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    updateState(bufferedElements)
    startState.update(start)
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

    startState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("start", classOf[Long]))

    if (context.isRestored) {
      val iterator = checkpointedState.keys().iterator()
      while (iterator.hasNext) {
        val key = iterator.next()
        bufferedElements(key) = checkpointedState.get(key)
      }
    }
  }

}