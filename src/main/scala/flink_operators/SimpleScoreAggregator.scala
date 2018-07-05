package flink_operators

import org.apache.flink.api.common.functions.AggregateFunction
import utils.ranking.{Score, SimpleScore}

/**
  * Aggregates incoming scores for given key to get
  * total score
  */
class SimpleScoreAggregator extends AggregateFunction[(Long, SimpleScore), Score, Score]{
  override def createAccumulator(): Score = SimpleScore(0)
  override def add(value: (Long, SimpleScore), accumulator: Score): Score = accumulator.add(value._2)
  override def getResult(accumulator: Score): Score = accumulator
  override def merge(a: Score, b: Score): Score = a.add(b)
}