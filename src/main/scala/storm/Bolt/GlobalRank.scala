package storm.Bolt

import java.util

import com.google.gson.Gson
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import utils.ranking.{RankElement, RankingResult}

class GlobalRank extends BaseRichBolt {

  var gson: Gson = new Gson()
  var _collector: OutputCollector = _

  var ranking: RankingResult[String] = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("globalRanking"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector

  override def execute(input: Tuple): Unit = {
    val rankingJson = input.getStringByField("PartialRanking")

    val partialRanking: RankingResult[String] = gson.fromJson(rankingJson, classOf[RankingResult[String]])
    val rankResult = new RankingResult[String](partialRanking.timestamp, partialRanking.rankElements, 10)

    if (ranking == null) {
      ranking = rankResult
    } else {
      ranking = ranking.mergeRank(rankResult)
    }

    var result: String = gson.toJson(rankResult)

    val value = new Values()
    value.add(result)

    _collector.emit(value)
    _collector.ack(input)
  }
}
