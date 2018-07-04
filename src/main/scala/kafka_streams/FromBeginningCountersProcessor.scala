package kafka_streams

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import utils.{Configuration, SerializerAny}

class FromBeginningCountersProcessor(punctuateTime: Long) extends Transformer[String, Array[Byte], (String, Array[Byte])] {

  private var stateFromBeginning: KeyValueStore[String, Array[Byte]] = _

  override def init(context: ProcessorContext): Unit =
    stateFromBeginning = context.getStateStore(Configuration.STATE_STORE_NAME).asInstanceOf[KeyValueStore[String, Array[Byte]]]


  override def transform(key: String, value: Array[Byte]): (String, Array[Byte]) = {

    val state = stateFromBeginning.get(key)

    var current_state : Array[Long] = Array.fill(25)(0l)
    if (state != null) {
      current_state = SerializerAny.deserialize(state).asInstanceOf[Array[Long]]
    }
    var init_timestamp = current_state(0)

    val actual_value = SerializerAny.deserialize(value).asInstanceOf[Array[Long]]
    if (init_timestamp == 0l || init_timestamp > actual_value(0)) { init_timestamp = actual_value(0) }

    val state_updated = current_state.zip(actual_value).map{case (x,y) => x+y}

    state_updated(0) = init_timestamp

    val new_value = SerializerAny.serialize(state_updated)
    stateFromBeginning.put(key, new_value)

    (key, new_value)
  }

  override def punctuate(timestamp: Long): (String, Array[Byte]) = {
    (Configuration.STATE_STORE_NAME, stateFromBeginning.get(Configuration.STATE_STORE_NAME))
  }

  override def close(): Unit = {
    // nothing to do
  }
}