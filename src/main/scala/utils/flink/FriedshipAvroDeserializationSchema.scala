package utils.flink

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import utils.kafka.KafkaAvroParser.recordInjectionFriendship


/**
  * @author emanuele 
  */
class FriedshipAvroDeserializationSchema extends DeserializationSchema[(String, String, String)] {

  override def deserialize(message: Array[Byte]): (String, String, String) = {
    val avro = recordInjectionFriendship.invert(message).get
    (avro.get("ts").toString, avro.get("user_id1").toString, avro.get("user_id2").toString)

  }


  override def isEndOfStream(nextElement: (String, String, String)): Boolean = false

  override def getProducedType: TypeInformation[(String, String, String)] = {
    TypeExtractor.createTypeInfo(classOf[(String, String, String)])
  }
}
