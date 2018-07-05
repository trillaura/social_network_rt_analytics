package flink_operators

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import utils.Parser
import utils.ranking.{GenericRankingBoard, GenericRankingResult, Score}



/**
  * A partial ranking operator that is combined with an
  * AggregatedFunction to incrementally aggregate elements
  * as they arrive in the current time window and update their ranking.
  * Outputs ranking only if it has changed from the previously sent one.
  * It also clear the ranking if the sliding time window advances
  */
class PartialRanker
  extends ProcessWindowFunction[Score, GenericRankingResult[Long], Long, TimeWindow]{

  /* keeps the partial ranking of the current operator instance */
  var partialRankBoard : GenericRankingBoard[Long] = new GenericRankingBoard[Long]()

  /* keeps the last time window start to compare it with new ones
   * and know when to clear ranking for new window */
  var lastTimeWindowStart : Long = 0


  /**
    * called when new value elements are ready
    * to be processed in the current time window
    * for the specific key
    * @param key  postID or userID
    * @param context
    * @param elements scores
    * @param out
    */
  override def process(key: Long,
                       context: Context,
                       elements: Iterable[Score],
                       out: Collector[GenericRankingResult[Long]]): Unit = {


    /* get current time window */
    val currentTimeWindow = context.window
    updateState(currentTimeWindow)

    incrementScore(key,elements.iterator.next())

    outputPartialRankingIfChanged(out, currentTimeWindow)

  }

  /**
    * outputs the current partial ranking to the next
    * operator only if the ranking has changed and it's
    * not empty
    * @param out collector that forwards data to the next operator
    * @param currentWindow time window
    */
  def outputPartialRankingIfChanged(out: Collector[GenericRankingResult[Long]], currentWindow: TimeWindow) = {
    if(partialRankBoard.rankHasChanged()) {

      val ranking = partialRankBoard.topK() /* Top-K in current window */
      if(ranking.nonEmpty) {
        /* sets the statistic's start timestamp as the current window's start time */
        val windowStartTimeStamp = Parser.convertToDateString(currentWindow.getStart)
        /* outputs the current partial Top-K ranking with its relative start timestamp */
        val output = new GenericRankingResult[Long](windowStartTimeStamp, ranking, partialRankBoard.K)
        out.collect(output)
      }
    }
  }

  /**
    * updates last time window start time if the current one
    * is starting after the last one and clears the
    * ranking board so to insert only scores within
    * the current window
    * @param currentTimeWindow TimeWindow
    */
  def updateState(currentTimeWindow: TimeWindow) = {

    if(currentTimeWindow.getStart > lastTimeWindowStart){
      lastTimeWindowStart = currentTimeWindow.getStart
      partialRankBoard.clear()
    }
  }

  /**
    * increments the ranking score associated
    * with the key
    * @param key
    * @param score
    */
  def incrementScore(key: Long, score: Score) = {
    partialRankBoard.incrementScoreBy(key, score)
  }

}
