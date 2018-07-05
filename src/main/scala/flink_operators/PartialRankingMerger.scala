package flink_operators

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import utils.ranking.GenericRankingResult

class PartialRankingMerger extends ProcessWindowFunction[GenericRankingResult[Long], GenericRankingResult[Long], String, TimeWindow]{

  /* keeps the last time window start to compare it with new ones
   * and know when to clear ranking for new window */
  var lastTimeWindowStart : Long = 0

  var currentRanking : GenericRankingResult[Long] = _
  var lastSentRanking : GenericRankingResult[Long] = _


  override def process(key: String,
                       context: Context,
                       elements: Iterable[GenericRankingResult[Long]],
                       out: Collector[GenericRankingResult[Long]]): Unit = {
    /* get current time window */
    val currentTimeWindow = context.window

    updateCurrentRanking(elements.iterator.next())

    if(differentWindow(currentTimeWindow)){
      if(lastSentRanking == null){
        lastSentRanking = currentRanking
      }
      if(lastSentRanking != currentRanking){
        out.collect(currentRanking)
        lastSentRanking = currentRanking
      }

      currentRanking = null
      lastTimeWindowStart = currentTimeWindow.getStart
    }

  }

  /**
    * checks whether we are in a new window
    * @param currentTimeWindow
    * @return
    */
  def differentWindow(currentTimeWindow: TimeWindow): Boolean = {
    currentTimeWindow.getStart > lastTimeWindowStart
  }

  /**
    * updates current window ranking by merging it
    * with new one
    * @param ranking newly arrived ranking
    */
  def updateCurrentRanking(ranking: GenericRankingResult[Long]) = {
    if(currentRanking == null){
      currentRanking = ranking
    } else {
      currentRanking = currentRanking mergeRank ranking
    }
  }

}