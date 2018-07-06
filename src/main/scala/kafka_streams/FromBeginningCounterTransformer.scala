package kafka_streams

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import utils.{Configuration, SerializerAny}

/**
  * Transformer operator to change a input stream to an output one,
  * keeping saved a state store for statistics from beginning of processing
  * time and the timestamp of start statistics computation updated at each
  * incoming tuple.
  *
  * @param punctuateTime time for next operation schedule
  */
class FromBeginningCounterTransformer(punctuateTime: Long) extends Transformer[String, Array[Byte], (String, Array[Byte])] {

  private var stateFromBeginning: KeyValueStore[String, Array[Byte]] = _
  private var minimumTimestamp: Long = _

  override def init(context: ProcessorContext): Unit = {
    stateFromBeginning = context.getStateStore(Configuration.STATE_STORE_NAME).asInstanceOf[KeyValueStore[String, Array[Byte]]]
    minimumTimestamp = Long.MaxValue
  }


  override def transform(key: String, value: Array[Byte]): (String, Array[Byte]) = {

    val state = stateFromBeginning.get(Configuration.STATE_STORE_NAME)

    var current_state : Array[Long] = Array.fill(25)(0l)
    if (state != null) {
      current_state = SerializerAny.deserialize(state).asInstanceOf[Array[Long]]
    }

    val incoming_value = SerializerAny.deserialize(value).asInstanceOf[Array[Long]]
    if (minimumTimestamp == Long.MaxValue || minimumTimestamp > incoming_value(0)) { minimumTimestamp = incoming_value(0) }

    val state_updated = current_state.zip(incoming_value).map{case (x,y) => x+y}

    state_updated(0) = minimumTimestamp

    val new_value = SerializerAny.serialize(state_updated)
    stateFromBeginning.put(Configuration.STATE_STORE_NAME, new_value)

    (Configuration.STATE_STORE_NAME, new_value)
  }

  override def punctuate(timestamp: Long): (String, Array[Byte]) = {
    (Configuration.STATE_STORE_NAME, stateFromBeginning.get(Configuration.STATE_STORE_NAME))
  }

  override def close(): Unit = {
    // nothing to do
  }
}