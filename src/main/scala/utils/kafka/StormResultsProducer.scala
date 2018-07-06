package utils.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import utils.Configuration

class StormResultsProducer(i: Int, t: String) {

  val topic: String = t
  val id: Int = i

  def produce(timestamp: Long, data: Array[Byte]) : Unit = {
    val record: ProducerRecord[Long, Array[Byte]] =
      new ProducerRecord(topic, timestamp, data)


  }
}
