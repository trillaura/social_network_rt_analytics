package flink_operators

import org.apache.flink.api.common.functions.ReduceFunction
import utils.ranking.GenericRankingResult

/**
  * Reduces Rankings by merging them
  */
class RankingResultsReducer extends ReduceFunction[GenericRankingResult[Long]]{
  override def reduce(value1: GenericRankingResult[Long], value2: GenericRankingResult[Long]): GenericRankingResult[Long] = {
    value1 mergeRank value2
  }
}