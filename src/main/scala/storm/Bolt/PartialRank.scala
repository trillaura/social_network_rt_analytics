package storm.Bolt

import java.util

import com.google.gson.Gson
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import utils.ranking.{RankElement, RankingBoard}

class PartialRank extends BaseRichBolt {

  var gson: Gson = _
  var _collector: OutputCollector = _

  val rankingBoard = new RankingBoard[String]()
  val lastWindow: Long = 0

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("timestamp", "partialRanking"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
    gson = new Gson()
  }

  override def execute(input: Tuple): Unit = {
    val timestamp: String = input.getStringByField("start")
    val postID: String = input.getStringByField("post_commented")
    val count: String = input.getStringByField("count")

    if (timestamp.toLong > lastWindow)
      rankingBoard.clear()

    rankingBoard.incrementScoreBy(postID, count.toInt)
    if (rankingBoard.rankHasChanged()) {
      val topk: List[RankElement[String]] = rankingBoard.topK()

      val value = new Values
      value.add(timestamp)
      value.add(topk)

      _collector.emit(value)
    }

    _collector.ack(input)
  }
}
