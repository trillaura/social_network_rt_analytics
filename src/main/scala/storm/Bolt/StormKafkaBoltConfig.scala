package storm.Bolt

import java.util.Properties

import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer}
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.bolt.mapper.{FieldNameBasedTupleToKafkaMapper, TupleToKafkaMapper}
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector
import utils.Configuration

object StormKafkaBoltConfig {

  def getKafkaBoltConfig : KafkaBolt[Long, Array[Byte]] = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", Configuration.BOOTSTRAP_SERVERS)
    props.put("acks", "1")
    props.put("key.serializer", classOf[LongSerializer])
    props.put("value.serializer", classOf[ByteArraySerializer])

    new KafkaBolt[Long, Array[Byte]]()
      .withProducerProperties(props)
      .withTopicSelector(new DefaultTopicSelector(Configuration.COMMENTS_OUTPUT_TOPIC_H1))
      .withTupleToKafkaMapper(new LongByteArrayTupleToKafkaMapper[Long, Array[Byte]]())
  }
}

import org.apache.storm.tuple.Tuple

class LongByteArrayTupleToKafkaMapper[Long, V] extends TupleToKafkaMapper[scala.Long, V] {
  private val serialVersionUID : scala.Long = -8794262989021702349L
  val BOLT_KEY: String = "timestamp"
  val BOLT_MESSAGE: String = "globalRanking"

  override def getKeyFromTuple(tuple: Tuple) : scala.Long =
    tuple.getStringByField(BOLT_KEY).toLong

  override def getMessageFromTuple(tuple: Tuple) : V =
    tuple.getValueByField(BOLT_MESSAGE).asInstanceOf[V]
}
