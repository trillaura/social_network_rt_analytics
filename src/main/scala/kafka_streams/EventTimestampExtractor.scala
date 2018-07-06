package kafka_streams

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

/**
  * The reference time is exacted from incoming record to used instead of default
  * Kafka tuple creation time.
  * Key-value couple of data read from Kafka, include in the key the timestamp
  * of record creation.
  */
class EventTimestampExtractor extends TimestampExtractor {

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {

    record.key.asInstanceOf[scala.Long]
  }
}
