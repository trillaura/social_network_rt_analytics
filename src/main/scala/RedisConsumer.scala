
import akka.actor.ActorSystem
import com.redis._
import utils.Configuration
import utils.kafka.ResultsConsumer
import utils.redis.RedisManager

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object RedisConsumer {

  /**
    * scala-redis is a blocking client for Redis. But you can develop high performance
    * asynchronous patterns of computation using scala-redis and Futures.
    * RedisClientPool allows you to work with multiple RedisClient instances and Futures
    * offer a non-blocking semantics on top of this. The combination can give you good
    * numbers for implementing common usage patterns like scatter/gather.
    * Here's an example that you will also find in the test suite. It uses the scatter/gather
    * technique to do loads of push across many lists in parallel. The gather phase pops from
    * all those lists in parallel and does some computation over them.
    */

  // set up Executors
//  val system = ActorSystem("ScatterGatherSystem")
//  import system.dispatcher
//
//  val timeout: Duration = 5 minutes
//
//  private[this] def flow[A](noOfRecipients: Int, opsPerClient: Int, keyPrefix: String,
//                            fn: (Int, String) => A) = {
//    (1 to noOfRecipients) map {i =>
//      Future {
//        fn(opsPerClient, keyPrefix + i)
//      }
//    }
//  }
//
//  // scatter across clients and gather them to do a sum
//  def scatterGatherWithList(opsPerClient: Int)(implicit clients: RedisClientPool) = {
//    // scatter
//    val futurePushes = flow(100, opsPerClient, "list_", listPush)
//
//    // concurrent combinator: Future.sequence
//    val allPushes = Future.sequence(futurePushes)
//
//    // sequential combinator: flatMap
//    val allSum = allPushes flatMap {result =>
//      // gather
//      val futurePops = flow(100, opsPerClient, "list_", listPop)
//      val allPops = Future.sequence(futurePops)
//      allPops map {members => members.sum}
//    }
//    Await.result(allSum, timeout).asInstanceOf[Long]
//  }
//
//  // scatter across clietns and gather the first future to complete
//  def scatterGatherFirstWithList(opsPerClient: Int)(implicit clients: RedisClientPool) = {
//    // scatter phase: push to 100 lists in parallel
//    val futurePushes = flow(100, opsPerClient, "seq_", listPush)
//
//    // wait for the first future to complete
//    val firstPush = Future.firstCompletedOf(futurePushes)
//
//    // do a sum on the list whose key we got from firstPush
//    val firstSum = firstPush map {key =>
//      listPop(opsPerClient, key)
//    }
//    Await.result(firstSum, timeout).asInstanceOf[Int]
//  }
//
//
//  // set
//  def zset(msgs: List[String]) = {
//    val clientPool = RedisManager.getDefaultRedisClientPool
//    clientPool.withClient {
//      client => {
//        var i = 0
//        msgs.foreach { v =>
//          client.set("key-%d".format(i), v)
//          i += 1
//        }
//        Some(1000)
//      }
//    }
//  }

  def main(args: Array[String]): Unit = {

    val TOPICS: List[String] = Configuration.OUTPUT_TOPICS
    val CONSUMERS_NUM: Int = TOPICS.length

    for (i <- 0 to CONSUMERS_NUM) {
      val c: Thread = new Thread{ new ResultsConsumer(i, TOPICS(i))}
      c.start()
    }

  }
}
