package utils.kafka

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.joda.time.DateTime
import utils.Configuration

object KafkaAvroParser {

  GenericData.get.addLogicalTypeConversion(new TimeConversions.TimestampConversion)

  val parser: Schema.Parser = new Schema.Parser()

  val schemaFriendship: Schema = parser.parse(Configuration.FRIENDSHIP_SCHEMA)
  val recordInjectionFriendship: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaFriendship)

  val schemaFriendshipResultsH24: Schema = parser.parse(Configuration.FRIENDSHIP_RESULT_SCHEMA_H24)
  val recordInjectionFriendshipResultsH24: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaFriendshipResultsH24)

  val schemaFriendshipResultsD7: Schema = parser.parse(Configuration.FRIENDSHIP_RESULT_SCHEMA_D7)
  val recordInjectionFriendshipResultsD7: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaFriendshipResultsD7)

  val schemaFriendshipResultsAllTime: Schema = parser.parse(Configuration.FRIENDSHIP_RESULT_SCHEMA_ALLTIME)
  val recordInjectionFriendshipResultsAllTime: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaFriendshipResultsAllTime)

  val schemaComment: Schema = parser.parse(Configuration.COMMENT_SCHEMA)
  val recordInjectionComment: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaComment)

  val schemaPost: Schema = parser.parse(Configuration.POST_SCHEMA)
  val recordInjectionPost: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaPost)


  def getRecordInjectionByTopic(topic: String) : Injection[GenericRecord, Array[Byte]] = {
  // TODO for all results schema
    if (topic.equals(Configuration.FRIENDS_OUTPUT_TOPIC_H24)) { recordInjectionFriendshipResultsH24 }
    else if (topic.equals(Configuration.FRIENDS_OUTPUT_TOPIC_D7)) { recordInjectionFriendshipResultsD7 }
    else { recordInjectionFriendshipResultsAllTime } // (topic.equals(Configuration.FRIENDS_OUTPUT_TOPIC_D7))
  }

  def fromByteArrayToResultRecord(record: Array[Byte], topic: String) : GenericRecord = {

    val recordInjection = getRecordInjectionByTopic(topic)
    recordInjection.invert(record).get
  }

  def fromByteArrayToFriendshipRecord(record: Array[Byte]) : GenericRecord = {
    recordInjectionFriendship.invert(record).get
  }

  def fromFriendshipRecordToByteArray(ts: DateTime, user1: Long, user2: Long) : Array[Byte] = {

    val avroRecord: GenericData.Record = new GenericData.Record(schemaFriendship)

    avroRecord.put("ts", ts)
    avroRecord.put("user_id1", user1)
    avroRecord.put("user_id2", user2)

    recordInjectionFriendship.apply(avroRecord)
  }

  def fromCommentRecordToByteArray(ts: DateTime, comment_id: Long, user_id: Long, comment: String,
                                   user: String, comment_replied: Boolean, post_commented: Long) : Array[Byte] = {

    val avroRecord: GenericData.Record = new GenericData.Record(schemaComment)

    avroRecord.put("ts", ts)
    avroRecord.put("comment_id", comment_id)
    avroRecord.put("user_id", user_id)
    avroRecord.put("comment", comment)
    avroRecord.put("user", user)
    avroRecord.put("comment_replied", comment_replied)
    avroRecord.put("post_commented", post_commented)

    recordInjectionComment.apply(avroRecord)
  }

  def fromPostRecordToByteArray(ts: DateTime, post_id: Long, user_id: Long, post: String, user: String) : Array[Byte] = {

    val avroRecord: GenericData.Record = new GenericData.Record(schemaPost)

    avroRecord.put("ts", ts)
    avroRecord.put("post_id", post_id)
    avroRecord.put("user_id", user_id)
    avroRecord.put("post", post)
    avroRecord.put("user", user)

    recordInjectionPost.apply(avroRecord)
  }
}
