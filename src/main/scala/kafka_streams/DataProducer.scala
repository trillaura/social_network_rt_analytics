package kafka_streams

import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.GenericData
import utils.{KafkaAvroParser, Parser}

object DataProducer {

  def computeInterval(frequency: Int, before: Long, now: Long) : Long = {
    if (before != 0l)
      return math.ceil(((now - before)/frequency).toDouble).toLong
    0l
  }

  def produceFriendships(frequency: Int) : Unit = {

    KafkaManager.createTopic(Configuration.FRIENDS_INPUT_TOPIC, 1, 1: Short)

    ProducerLauncher.createAvroProducer()

    GenericData.get.addLogicalTypeConversion(new TimeConversions.TimestampConversion)

    val friendships = Parser.readFriendships(Configuration.DATASET_FRIENDSHIPS)

    var before: Long = 0l
    var period: Long = 0l

    for (c <- friendships) {

      val now : Long = c.timestamp.toInstant.getMillis

      period = computeInterval(frequency, before, now)

      val bytes: Array[Byte] =
        KafkaAvroParser.fromFriendshipRecordToByteArray(c.timestamp, c.firstUser.id, c.secondUser.id)

      Thread.sleep(period)

      before = now
      ProducerLauncher.produceAvro(bytes)

    }
    ProducerLauncher.closeAvro()
  }

  def produceComments(frequency: Int) : Unit = {

    KafkaManager.createTopic(Configuration.COMMENTS_INPUT_TOPIC, 1, 1: Short)

    ProducerLauncher.createAvroProducer()

    GenericData.get.addLogicalTypeConversion(new TimeConversions.TimestampConversion)

    val comments = Parser.readComments(Configuration.DATASET_COMMENTS)

    var before: Long = 0l
    var period: Long = 0l

    for (c <- comments) {

      val now : Long = c.timestamp.toInstant.getMillis

      period = computeInterval(frequency, before, now)

      val bytes: Array[Byte] =
        KafkaAvroParser.fromCommentRecordToByteArray(c.timestamp, c.id, c.user.id, c.content, c.user.name, c.postComment, c.parentID)

      Thread.sleep(period)

      before = now

      ProducerLauncher.produceAvro(bytes)

    }
    ProducerLauncher.closeAvro()
  }

  def producePosts(frequency: Int) : Unit = {

    KafkaManager.createTopic(Configuration.POSTS_INPUT_TOPIC, 1, 1: Short)

    ProducerLauncher.createAvroProducer()

    GenericData.get.addLogicalTypeConversion(new TimeConversions.TimestampConversion)

    val posts = Parser.readPosts(Configuration.DATASET_POSTS)

    var before: Long = 0l
    var period: Long = 0l

    for (p <- posts) {

      val now : Long = p.timestamp.toInstant.getMillis

      period = computeInterval(frequency, before, now)

      val bytes: Array[Byte] =
        KafkaAvroParser.fromPostRecordToByteArray(p.timestamp, p.id, p.user.id, p.content, p.user.name)

      Thread.sleep(period)

      before = now

      ProducerLauncher.produceAvro(bytes)

    }
    ProducerLauncher.closeAvro()
  }

  def main(args: Array[String]): Unit = {
//    produceFriendships(10000000)
//    produceComments(10000000)
    producePosts(10000000)
  }

}
