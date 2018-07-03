package utils.flink

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class FilterFunction extends RichFlatMapFunction[(String,String,String),(String,String,String)] {

  private var seen: ValueState[Boolean] = _

  override def flatMap(value: (String,String,String), out: Collector[(String,String,String)]): Unit = {

    if (!seen.value()) {
      seen.update(true)
      out.collect(value)
    }
  }

  override def open(parameters: Configuration): Unit = {
    seen = getRuntimeContext.getState(
      new ValueStateDescriptor("seen", classOf[Boolean])
    )
  }
}
