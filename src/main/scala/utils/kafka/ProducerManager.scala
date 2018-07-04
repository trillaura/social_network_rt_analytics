package utils.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer, StringSerializer}
import utils.Configuration

object ProducerManager {

  var producer: Producer[Long, Array[Byte]] = createProducer()

  def createProducer() : Producer[Long, Array[Byte]] = {
    val props: Properties  = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, Configuration.PRODUCER_ID)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new LongSerializer().getClass.getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)

    new KafkaProducer[Long, Array[Byte]](props)
  }

  def getDefaultProducer : Producer[Long, Array[Byte]] =
    this.producer

}
