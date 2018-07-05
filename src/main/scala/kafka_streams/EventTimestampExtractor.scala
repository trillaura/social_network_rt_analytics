package kafka_streams

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

/**
  *
  */
class EventTimestampExtractor extends TimestampExtractor {

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {

    record.key.asInstanceOf[scala.Long]
  }
}
