package utils.kafka

import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import utils.{Configuration, Parser}


class ProducerLauncher(t: String, f: Int) {

  val producer_id: String = Configuration.PRODUCER_ID
  val topic: String = t
  var producer: Producer[String, Array[Byte]] = createProducer()

  val frequency: Int = f

  val parser: Schema.Parser = new Schema.Parser()

  def createProducer(): Producer[String, Array[Byte]] = {

    val props: Properties  = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, Configuration.PRODUCER_ID)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)

    new KafkaProducer[String, Array[Byte]](props)
  }

  def produce(data: Array[Byte], topic: String) : Unit = {

    val record: ProducerRecord[String, Array[Byte]] = new ProducerRecord(topic, data)

    val metadata: RecordMetadata = producer.send(record).get()

    // DEBUG
    printf("sent avro record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
      record.key(), record.value(), metadata.partition(), metadata.offset())
  }

  def close(): Unit = {
    producer.flush()
    producer.close()
  }


  def computeInterval(frequency: Int, before: Long, now: Long) : Long = {
    if (before != 0l)
      return math.ceil(((now - before)/frequency).toDouble).toLong
    0l
  }

  def produceFriendships(frequency: Int) : Unit = {

    GenericData.get.addLogicalTypeConversion(new TimeConversions.TimestampConversion)

    val friendships = Parser.readFriendships(Configuration.DATASET_FRIENDSHIPS)

    var before: Long = 0l
    var period: Long = 0l

    for (c <- friendships) {

      val now : Long = c.timestamp.toInstant.getMillis

      period = computeInterval(frequency, before, now)

      println(c.timestamp)

      val bytes: Array[Byte] =
        KafkaAvroParser.fromFriendshipRecordToByteArray(c.timestamp, c.firstUser.id, c.secondUser.id)

      Thread.sleep(period)

      before = now
      produce(bytes, topic)

    }
    close()
  }

  def produceComments(frequency: Int) : Unit = {

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

      produce(bytes, topic)

    }
    close()
  }

  def producePosts(frequency: Int) : Unit = {

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

      produce(bytes, topic)

    }
    close()
  }


  def run() : Unit = {
    if (topic.equals(Configuration.FRIENDS_INPUT_TOPIC)) { produceFriendships(frequency) }
    else if (topic.equals(Configuration.COMMENTS_INPUT_TOPIC)) { produceComments(frequency) }
    else if (topic.equals(Configuration.POSTS_INPUT_TOPIC)) { producePosts(frequency) }
  }
}
