package storm.Bolt

import java.util

import com.google.gson.Gson
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import utils.ranking.{RankElement, RankingBoard, RankingResult}

class PartialRank extends BaseRichBolt {

  var gson: Gson = new Gson()
  var _collector: OutputCollector = _

  val rankingBoard = new RankingBoard[String]()

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("partialRanking"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector

  override def execute(input: Tuple): Unit = {
    val timestamp: String = input.getStringByField("start")
    val postID: String = input.getStringByField("postID")
    val count: String = input.getStringByField("count")

    rankingBoard.updateTop(postID, count.toInt)
    if (rankingBoard.rankHasChanged()) {
      val topk: List[RankElement[String]] = rankingBoard.topK()
      val partialRanking = new RankingResult[String](timestamp, topk, 10)
      val ranking = gson.toJson(partialRanking)

      val value = new Values
      value.add(ranking)

      _collector.emit(value)
    }

    _collector.ack(input)
  }
}
