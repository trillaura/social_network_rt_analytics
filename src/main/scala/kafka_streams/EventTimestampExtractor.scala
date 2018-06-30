package kafka_streams

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import utils.Parser
import utils.kafka.KafkaAvroParser

/**
  *
  */
class EventTimestampExtractor extends TimestampExtractor {

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {

    val avroRec: GenericRecord = KafkaAvroParser.fromByteArrayToFriendshipRecord(record.value.asInstanceOf[Array[Byte]])

    Parser.convertToDateTime(avroRec.get("ts").toString).getMillis
  }
}
