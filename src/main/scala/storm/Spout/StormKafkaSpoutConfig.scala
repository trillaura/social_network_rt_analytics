package storm.Spout

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.storm.kafka.spout.{DefaultRecordTranslator, KafkaSpoutConfig}
import utils.Configuration

object StormKafkaSpoutConfig {

  def getSpoutConfig : KafkaSpoutConfig[String, String] = {
    KafkaSpoutConfig.builder(Configuration.BOOTSTRAP_SERVERS, Configuration.COMMENTS_INPUT_TOPIC)
      .setPollTimeoutMs(1000)
      .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
      .setRecordTranslator(new DefaultRecordTranslator())
      .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      .build()
  }
}