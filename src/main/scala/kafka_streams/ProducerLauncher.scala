package kafka_streams

import java.util.Properties

import org.apache.avro.Schema
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}


object ProducerLauncher {

  val SLEEP: Int = 1000

  var producerAvro: Producer[String, Array[Byte]] = createAvroProducer()

  val parser: Schema.Parser = new Schema.Parser()
  val schemaFriend: Schema = parser.parse(Configuration.FRIENDSHIP_SCHEMA)

  def createAvroProducer(): Producer[String, Array[Byte]] = {

    val props: Properties  = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, Configuration.PRODUCER_ID)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)

    new KafkaProducer[String, Array[Byte]](props)
  }

  def produceAvro(data: Array[Byte], topic: String) : Unit = {

    val record: ProducerRecord[String, Array[Byte]] = new ProducerRecord(topic, data)

    val metadata: RecordMetadata = producerAvro.send(record).get()

    // DEBUG
    printf("sent avro record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
      record.key(), record.value(), metadata.partition(), metadata.offset())
  }

  def closeAvro(): Unit = {
    producerAvro.flush()
    producerAvro.close()
  }
}
