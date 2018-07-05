package flink_operators

import org.apache.flink.api.common.functions.AggregateFunction
import utils.ranking.{Score, UserScore}

/**
  * Aggregates incoming scores for given key to get
  * total score
  */
class UserScoreAggregator extends AggregateFunction[(Long, UserScore), Score, Score]{
  override def createAccumulator(): Score = UserScore(0,0,0)
  override def add(value: (Long, UserScore), accumulator: Score): Score = accumulator.add(value._2)
  override def getResult(accumulator: Score): Score = accumulator
  override def merge(a: Score, b: Score): Score = a.add(b)
}
