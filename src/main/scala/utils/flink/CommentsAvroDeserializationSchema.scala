package utils.flink

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import utils.kafka.KafkaAvroParser.recordInjectionComment

class CommentsAvroDeserializationSchema extends DeserializationSchema[(String, String, String, String, String, String, String)] {
  override def deserialize(message: Array[Byte]): (String, String, String, String, String, String, String) = {
    val avro = recordInjectionComment.invert(message).get
    (avro.get("ts").toString, avro.get("comment_id").toString,
      avro.get("user_id").toString, avro.get("comment").toString,
      avro.get("user").toString, avro.get("comment_replied").toString,
      avro.get("post_commented").toString)
  }

  override def isEndOfStream(nextElement: (String, String, String, String, String, String, String)): Boolean = false

  override def getProducedType: TypeInformation[(String, String, String, String, String, String, String)] = {
    TypeExtractor.createTypeInfo(classOf[(String, String, String, String, String, String, String)])
  }
}
