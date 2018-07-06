package storm.Bolt

import java.util

import com.google.gson.Gson
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import utils.Parser
import utils.ranking.{RankElement, RankingResult}

/**
  * It aggregates partial ranking from the previous bolt in order to output a single global ranking.
  * It emit data to the final bolt in charge of collect result.
  */
class GlobalRank extends BaseRichBolt {

  var gson: Gson = _
  var _collector: OutputCollector = _

  var ranking: RankingResult[String] = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("timestamp", "globalRanking"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
    gson = new Gson()
  }

  override def execute(input: Tuple): Unit = {
    val partialRanking = input.getValueByField("partialRanking").asInstanceOf[List[RankElement[String]]]
    val timestamp = input.getStringByField("timestamp")

    // Convert unix timestamp in dateTime
    val date: String = Parser.convertToDateTime(timestamp.toLong).toString()
    val rankResult = new RankingResult[String](date, partialRanking, 10)

    if (ranking == null || ranking.timestamp < rankResult.timestamp) {
      ranking = rankResult
    } else {
      ranking = ranking.mergeRank(rankResult) // merge partial ranks
    }

    val value = new Values()
    value.add(date)
    value.add(ranking.rankElements)

    _collector.emit(value)
    _collector.ack(input)
  }
}
