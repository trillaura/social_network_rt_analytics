package storm.Bolt

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple}
import utils.ranking.{RankElement, RankingBoard}

import scala.util.parsing.json.JSON

class PartialRank extends BaseRichBolt {
  var _collector: OutputCollector = _

  val rankingBoard = new RankingBoard[String]()

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("ts", "ranking"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector

  override def execute(input: Tuple): Unit = {
    val timestamp = input.getStringByField("start")
    val postID = input.getStringByField("postID")
    val count = input.getStringByField("count")

    rankingBoard.updateTop(postID, count.toInt)
    if (rankingBoard.rankHasChanged()) {
      val topk: Array[RankElement[String]] = rankingBoard.topK().toArray
    }
  }
}
