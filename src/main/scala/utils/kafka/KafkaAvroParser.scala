package utils.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.io.JsonDecoder._
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.joda.time.DateTime
import utils.Configuration
import utils.ranking.RankElement

object KafkaAvroParser extends Serializable {

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

  val schemaCommentResultsH1: Schema = parser.parse(Configuration.COMMENT_RESULT_SCHEMA_H1)
  val recordInjectionCommentsResultsH1: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaCommentResultsH1)

  val schemaCommentsResultsH24: Schema = parser.parse(Configuration.COMMENT_RESULT_SCHEMA_H24)
  val recordInjectionCommentsResultsH24: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaCommentsResultsH24)

  val schemaCommentsResultsD7: Schema = parser.parse(Configuration.COMMENT_RESULT_SCHEMA_D7)
  val recordInjectionCommentsResultsD7: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaCommentsResultsD7)

  val schemaPostsResultsH1: Schema = parser.parse(Configuration.POST_RESULT_SCHEMA_H1)
  val recordInjectionPostsResultsH1: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaPostsResultsH1)

  val schemaPostsResultsH24: Schema = parser.parse(Configuration.POST_RESULT_SCHEMA_H24)
  val recordInjectionPostsResultsH24: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaPostsResultsH24)

  val schemaPostsResultsD7: Schema = parser.parse(Configuration.POST_RESULT_SCHEMA_D7)
  val recordInjectionPostsResultsD7: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaPostsResultsD7)

  val schemaComment: Schema = parser.parse(Configuration.COMMENT_SCHEMA)
  val recordInjectionComment: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaComment)

  val schemaPost: Schema = parser.parse(Configuration.POST_SCHEMA)
  val recordInjectionPost: Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary(schemaPost)


  def getRecordInjectionByTopic(topic: String) : Injection[GenericRecord, Array[Byte]] = {
    if (topic.equals(Configuration.FRIENDS_OUTPUT_TOPIC_H24)) { recordInjectionFriendshipResultsH24 }
    else if (topic.equals(Configuration.FRIENDS_OUTPUT_TOPIC_D7)) { recordInjectionFriendshipResultsD7 }
    else if (topic.equals(Configuration.FRIENDS_OUTPUT_TOPIC_D7)) { recordInjectionFriendshipResultsAllTime }
    else if (topic.equals(Configuration.COMMENTS_OUTPUT_TOPIC_H1)) { recordInjectionCommentsResultsH1 }
    else if (topic.equals(Configuration.COMMENTS_OUTPUT_TOPIC_H24)) { recordInjectionCommentsResultsH24 }
    else if (topic.equals(Configuration.COMMENTS_OUTPUT_TOPIC_7D)) { recordInjectionCommentsResultsD7 }
    else if (topic.equals(Configuration.POSTS_OUTPUT_TOPIC_H1)) { recordInjectionPostsResultsH1 }
    else if (topic.equals(Configuration.POSTS_OUTPUT_TOPIC_H24)) { recordInjectionPostsResultsH24 }
    else if (topic.equals(Configuration.POSTS_OUTPUT_TOPIC_7D)) { recordInjectionPostsResultsD7 }
    else if (topic.equals(Configuration.FRIENDS_INPUT_TOPIC)) { recordInjectionFriendship }
    else if (topic.equals(Configuration.COMMENTS_INPUT_TOPIC)) { recordInjectionComment }
    else { recordInjectionPost } // if (topic.equals(Configuration.POSTS_INPUT_TOPIC))
  }

  def fromByteArrayToRecord(record: Array[Byte], topic: String) : GenericRecord = {

    val recordInjection = getRecordInjectionByTopic(topic)
    recordInjection.invert(record).get
  }

  def fromByteArrayToFriendshipRecord(record: Array[Byte]) : GenericRecord =
    recordInjectionFriendship.invert(record).get

  def fromByteArrayToCommentRecord(record: Array[Byte]) : GenericRecord = {
    val objMapper = new ObjectMapper()
    val value = objMapper.readValue(record, classOf[Array[Byte]])
    recordInjectionComment.invert(value).get
  }

  def fromByteArrayToPostRecord(record: Array[Byte]) : GenericRecord =
    recordInjectionPost.invert(record).get

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

  def fromFriendshipsResultsRecordToByteArray(ts: Long, counters: Array[scala.Long], schema: Schema): Array[Byte] = {


    val avroRecord: GenericData.Record = new GenericData.Record(schema)

    for (i <- counters.indices) {
      if (ts == 0l || counters.length == 25) {
        if (i == 0)
          avroRecord.put("ts", counters(0))
        else if (i <= 10)
          avroRecord.put("count_h0" + (i-1).toString, counters(i))
        else
          avroRecord.put("count_h" + (i-1).toString, counters(i))
      } else {
        avroRecord.put("ts", ts)
        if (i < 10)
          avroRecord.put("count_h0" + i.toString, counters(i))
        else
          avroRecord.put("count_h" + i.toString, counters(i))
      }
    }

    if (schema.equals(schemaFriendshipResultsH24)) {
      recordInjectionFriendshipResultsH24.apply(avroRecord)
    } else if (schema.equals(schemaFriendshipResultsD7)) {
      recordInjectionFriendshipResultsD7.apply(avroRecord)
    } else {
      recordInjectionFriendshipResultsAllTime.apply(avroRecord)
    }
  }

  def fromFriendshipsResultsRecordToByteArray(ts: Long, counters: Array[scala.Long], topic: String): Array[Byte] = {

    var schema : Schema = schemaFriendshipResultsH24

    if (topic.equals(Configuration.FRIENDS_OUTPUT_TOPIC_D7))
      schema = schemaFriendshipResultsD7

    if (topic.equals(Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME))
      schema = schemaFriendshipResultsAllTime

    val avroRecord: GenericData.Record = new GenericData.Record(schema)

    for (i <- counters.indices) {
      if (ts == 0l || counters.length == 25) {
        if (i == 0)
          avroRecord.put("ts", counters(0))
        else if (i <= 10)
          avroRecord.put("count_h0" + (i-1).toString, counters(i))
        else
          avroRecord.put("count_h" + (i-1).toString, counters(i))
      } else {
        avroRecord.put("ts", ts)
        if (i < 10)
          avroRecord.put("count_h0" + i.toString, counters(i))
        else
          avroRecord.put("count_h" + i.toString, counters(i))
      }
    }

    if (schema.equals(schemaFriendshipResultsH24)) {
      recordInjectionFriendshipResultsH24.apply(avroRecord)
    } else if (schema.equals(schemaFriendshipResultsD7)) {
      recordInjectionFriendshipResultsD7.apply(avroRecord)
    } else {
      recordInjectionFriendshipResultsAllTime.apply(avroRecord)
    }
  }

  def fromCommentsResultsRecordToByteArray(ts: Long,
                                           post_id_1: Long, num_comments_1: Long,
                                           post_id_2: Long, num_comments_2: Long,
                                           post_id_3: Long, num_comments_3: Long,
                                           post_id_4: Long, num_comments_4: Long,
                                           post_id_5: Long, num_comments_5: Long,
                                           post_id_6: Long, num_comments_6: Long,
                                           post_id_7: Long, num_comments_7: Long,
                                           post_id_8: Long, num_comments_8: Long,
                                           post_id_9: Long, num_comments_9: Long,
                                           post_id_10: Long, num_comments_10: Long, schema: Schema) : Array[Byte] = {

    val avroRecord: GenericData.Record = new GenericData.Record(schema)

    avroRecord.put("ts", ts)
    avroRecord.put("post_id_1", post_id_1)
    avroRecord.put("num_comments_1", num_comments_1)
    avroRecord.put("post_id_2", post_id_2)
    avroRecord.put("num_comments_2", num_comments_2)
    avroRecord.put("post_id_3", post_id_3)
    avroRecord.put("num_comments_3", num_comments_3)
    avroRecord.put("post_id_4", post_id_4)
    avroRecord.put("num_comments_4", num_comments_4)
    avroRecord.put("post_id_5", post_id_5)
    avroRecord.put("num_comments_5", num_comments_5)
    avroRecord.put("post_id_6", post_id_6)
    avroRecord.put("num_comments_6", num_comments_6)
    avroRecord.put("post_id_7", post_id_7)
    avroRecord.put("num_comments_7", num_comments_7)
    avroRecord.put("post_id_8", post_id_8)
    avroRecord.put("num_comments_8", num_comments_8)
    avroRecord.put("post_id_9", post_id_9)
    avroRecord.put("num_comments_9", num_comments_9)
    avroRecord.put("post_id_10", post_id_10)
    avroRecord.put("num_comments_10", num_comments_10)

    if (schema.equals(schemaCommentResultsH1)) {
      recordInjectionCommentsResultsH1.apply(avroRecord)
    } else if (schema.equals(schemaCommentsResultsH24)) {
      recordInjectionCommentsResultsH24.apply(avroRecord)
    } else {
      recordInjectionCommentsResultsD7.apply(avroRecord)
    }
  }

  def fromCommentsResultsRecordToByteArray(ts: Long, elements: Array[RankElement[String]], schema: Schema) : Array[Byte] = {

    val avroRecord: GenericData.Record = new GenericData.Record(schema)

    avroRecord.put("ts", ts)
    for (i <- elements.indices) {
      avroRecord.put("post_id_" + (i+1).toString, elements(i).id.toLong)
      avroRecord.put("num_comments_" + (i+1).toString, elements(i).score.toLong)
    }

    if (elements.length < 10) {
      for (i <- elements.length to 9) {
        avroRecord.put("post_id_" + (i+1).toString, 0l)
        avroRecord.put("num_comments_" + (i+1).toString, 0l)
      }
    }

    if (schema.equals(schemaCommentResultsH1)) {
      recordInjectionCommentsResultsH1.apply(avroRecord)
    } else if (schema.equals(schemaCommentsResultsH24)) {
      recordInjectionCommentsResultsH24.apply(avroRecord)
    } else {
      recordInjectionCommentsResultsD7.apply(avroRecord)
    }
  }
}
