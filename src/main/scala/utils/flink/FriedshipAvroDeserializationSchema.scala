package utils.flink

import org.apache.avro.Schema
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import utils.kafka.KafkaAvroParser
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

class ResultAvroSerializationSchema(t: String) extends SerializationSchema[(Long, Array[Int])] {

  var topic : String = t

  override def serialize(element: (Long, Array[Int])): Array[Byte] = {
    val counters = Array.fill(element._2.length)(0l)
    for (i <- element._2.indices)
      counters(i) = element._2(i).toLong
    KafkaAvroParser.fromFriendshipsResultsRecordToByteArray(element._1, counters, topic)
  }
}
