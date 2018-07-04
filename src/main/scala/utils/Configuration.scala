package utils

import org.joda.time.format.DateTimeFormat

object Configuration {

  val REDIS_HOST: String = "localhost"
  val REDIS_PORT: Int = 6379

  val STATE_STORE_NAME: String = "from-beginning-counters"

  val TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
  private lazy val dateFormatter = DateTimeFormat.forPattern(TIMESTAMP_FORMAT)

  val APP_ID: String = "social-network-analysis-app"
  val CLIENT_ID: String = "social-network-analysis-app-client"

  val BOOTSTRAP_SERVERS: String = "localhost:9092,localhost:9093,localhost:9094"
  val ZOOKEEPER_SERVERS: String = "localhost:2181,localhost:2888,localhost:3888"

  val DATASET_FRIENDSHIPS: String = "dataset/friendships.dat"
  val DATASET_POSTS: String = "dataset/posts.dat"
  val DATASET_COMMENTS: String = "dataset/comments.dat"

  val TEST_DATASET_FRIENDSHIPS: String = "dataset/test_friendships.dat"

  val FRIENDS_INPUT_TOPIC: String = "friendships-stream-input"
  val FRIENDS_OUTPUT_TOPIC_H24: String = "friendships-h24-stream-output"
  val FRIENDS_OUTPUT_TOPIC_D7: String = "friendships-d7-stream-output"
  val FRIENDS_OUTPUT_TOPIC_ALLTIME: String = "friendships-alltime-stream-output"

  val POSTS_INPUT_TOPIC: String = "posts-stream-input"
  val POSTS_OUTPUT_TOPIC_H24: String = "posts-h24-stream-output"
  val POSTS_OUTPUT_TOPIC_7D: String = "posts-d7-stream-output"
  val POSTS_OUTPUT_TOPIC_H1: String = "posts-h1-stream-output"

  val COMMENTS_INPUT_TOPIC: String = "comments-stream-input"
  val COMMENTS_OUTPUT_TOPIC_H24: String = "comments-h24-stream-output"
  val COMMENTS_OUTPUT_TOPIC_7D: String = "comments-d7-stream-output"
  val COMMENTS_OUTPUT_TOPIC_H1: String = "comments-h1-stream-output"

  val INPUT_TOPICS : List[String] = List(FRIENDS_INPUT_TOPIC, COMMENTS_INPUT_TOPIC, POSTS_INPUT_TOPIC)
  val OUTPUT_TOPICS : List[String] = List(
    FRIENDS_OUTPUT_TOPIC_H24, FRIENDS_OUTPUT_TOPIC_D7, FRIENDS_OUTPUT_TOPIC_ALLTIME,
    COMMENTS_OUTPUT_TOPIC_H24, COMMENTS_OUTPUT_TOPIC_7D, COMMENTS_OUTPUT_TOPIC_H1,
    POSTS_OUTPUT_TOPIC_H24, POSTS_OUTPUT_TOPIC_7D, POSTS_OUTPUT_TOPIC_H1)

  val CONSUMER_GROUP_ID: String = "app-consumer1"
  val NUM_CONSUMERS: Int = 3

  val PRODUCER_ID : String = "app-producer1"

  val FRIENDSHIP_SCHEMA: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"friendship_record\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": {" +
    "     \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }" +
    "  }," +
    "  { \"name\":\"user_id1\", \"type\":\"long\" }," +
    "  { \"name\":\"user_id2\", \"type\":\"long\" }" +
    "]}"

  val FRIENDSHIP_RESULT_SCHEMA_H24: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"friendship_statH24\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": \"long\" }," +
    "  { \"name\":\"count_h00\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h01\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h02\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h03\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h04\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h05\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h06\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h07\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h08\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h09\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h10\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h11\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h12\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h13\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h14\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h15\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h16\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h17\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h18\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h19\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h20\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h21\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h22\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h23\", \"type\":\"long\" }"  +
    "]}"

  val FRIENDSHIP_RESULT_SCHEMA_D7: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"friendship_statD7\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": \"long\" }," +
    "  { \"name\":\"count_h00\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h01\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h02\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h03\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h04\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h05\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h06\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h07\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h08\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h09\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h10\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h11\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h12\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h13\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h14\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h15\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h16\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h17\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h18\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h19\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h20\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h21\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h22\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h23\", \"type\":\"long\" }"  +
    "]}"

  val FRIENDSHIP_RESULT_SCHEMA_ALLTIME: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"friendship_statALLTIME\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": \"long\" }," +
    "  { \"name\":\"count_h00\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h01\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h02\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h03\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h04\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h05\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h06\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h07\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h08\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h09\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h10\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h11\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h12\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h13\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h14\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h15\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h16\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h17\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h18\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h19\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h20\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h21\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h22\", \"type\":\"long\" }," +
    "  { \"name\":\"count_h23\", \"type\":\"long\" }"  +
    "]}"

  val COMMENT_SCHEMA: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"comment_record\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": {" +
    "     \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }" +
    "  }," +
    "  { \"name\":\"comment_id\", \"type\":\"long\" }," +
    "  { \"name\":\"user_id\", \"type\":\"long\" }," +
    "  { \"name\":\"comment\", \"type\":\"string\" }," +
    "  { \"name\":\"user\", \"type\":\"string\" }," +
    "  { \"name\":\"comment_replied\", \"type\":\"boolean\" }," +
    "  { \"name\":\"post_commented\", \"type\":\"long\" }" +
    "]}"

  val POST_SCHEMA: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"post_record\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": {" +
    "     \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }" +
    "  }," +
    "  { \"name\":\"post_id\", \"type\":\"long\" }," +
    "  { \"name\":\"user_id\", \"type\":\"long\" }," +
    "  { \"name\":\"post\", \"type\":\"string\" }," +
    "  { \"name\":\"user\", \"type\":\"string\" }" +
    "]}"


  val COMMENT_RESULT_SCHEMA_H1: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"comments_statH1\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": \"long\" }," +
    "  { \"name\":\"post_id_1\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_1\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_2\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_2\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_3\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_3\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_4\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_4\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_5\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_5\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_6\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_6\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_7\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_7\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_8\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_8\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_9\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_9\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_10\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_10\", \"type\":\"long\" }," +
    "]}"

  val COMMENT_RESULT_SCHEMA_H24: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"comments_statH24\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": \"long\" }," +
    "  { \"name\":\"post_id_1\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_1\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_2\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_2\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_3\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_3\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_4\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_4\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_5\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_5\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_6\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_6\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_7\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_7\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_8\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_8\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_9\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_9\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_10\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_10\", \"type\":\"long\" }," +
    "]}"

  val COMMENT_RESULT_SCHEMA_D7: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"comments_statD7\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\": \"long\" }," +
    "  { \"name\":\"post_id_1\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_1\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_2\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_2\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_3\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_3\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_4\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_4\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_5\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_5\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_6\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_6\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_7\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_7\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_8\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_8\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_9\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_9\", \"type\":\"long\" }," +
    "  { \"name\":\"post_id_10\", \"type\":\"long\" }," +
    "  { \"name\":\"num_comments_10\", \"type\":\"long\" }," +
    "]}"

}
