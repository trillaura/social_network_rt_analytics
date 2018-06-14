package kafka_streams

import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs


object ProducerLauncher {

  val SLEEP: Int = 1000

  var producer: Producer[String,String] = createProducer()
  var producerAvro: Producer[String, Array[Byte]] = createAvroProducer()

  val parser: Schema.Parser = new Schema.Parser()
  val schema: Schema = parser.parse(Configuration.FRIENDSHIP_SCHEMA)

  def createProducer(): Producer[String, String] = {

    val props: Properties  = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, Configuration.PRODUCER_ID)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)

    new KafkaProducer[String, String](props)
  }

  def createAvroProducer(): Producer[String, Array[Byte]] = {

    val props: Properties  = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, Configuration.PRODUCER_ID)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)

    new KafkaProducer[String, Array[Byte]](props)
  }

  def produce(key: String, value: String) : Unit = {

    val record: ProducerRecord[String, String] =
        new ProducerRecord[String, String](Configuration.INPUT_TOPIC, key, value)

    val metadata: RecordMetadata = producer.send(record).get()

      // DEBUG
      printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
        record.key(), record.value(), metadata.partition(), metadata.offset())
  }

  def produceAvro(data: Array[Byte]) : Unit = {

    val record: ProducerRecord[String, Array[Byte]] = new ProducerRecord(
      Configuration.INPUT_TOPIC, data)
    producerAvro.send(record)

    Thread.sleep(250)
//    val record: ProducerRecord[String, ByteArray] =
//      new ProducerRecord[String, ByteArray](Configuration.INPUT_TOPIC, key, value)

    val metadata: RecordMetadata = producerAvro.send(record).get()

    // DEBUG
    printf("sent avro record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
      record.key(), record.value(), metadata.partition(), metadata.offset())
  }

  def closeAvro(): Unit = {
    producerAvro.flush()
    producerAvro.close()
  }

  def close(): Unit = {
    producer.flush()
    producer.close()
  }

  def main(args: Array[String]): Unit = {

    for (i <- 0 until 100) {
      val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

      val avroRecord: GenericData.Record = new GenericData.Record(schema)
      avroRecord.put("ts", System.currentTimeMillis())
      avroRecord.put("user_id1", System.currentTimeMillis() % 51)
      avroRecord.put("user_id2", System.currentTimeMillis() % 51)

      val bytes: Array[Byte] = recordInjection.apply(avroRecord)
      produceAvro(bytes)
      Thread.sleep(SLEEP)
    }
    closeAvro()
//    for (i <- 0 until 1000) {
//        val timestamp: Long = System.currentTimeMillis()
//        val payload: String = "Hello world at " + timestamp
//        produce(null, payload)
//        Thread.sleep(SLEEP)
//    }
//    close()
  }
}
