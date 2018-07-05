package flink_operators

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import utils.Parser
import utils.ranking.{GenericRankingResult, GlobalRankingHolder}

/**
  * Merges partial ranks into global one
  * coming from different operators.
  * It also keeps only recent partial ranks,
  * periodically discarding old ones
  */
class GlobalRanker extends ProcessFunction[GenericRankingResult[Long], GenericRankingResult[Long]] {


  //private var hashMap =  mutable.HashMap[String,  utils.ranking.GenericRankingResult[Long]]()

  /* for how much millis to keep partial rankings */
  private val delta = 1000 * 60 * 10 /* 10 minutes */

  /* keeps the partial rankings and computes global one */
  private var globalRankingHolder: GlobalRankingHolder[Long] = new GlobalRankingHolder[Long](delta)

  /* keeps last seen timestamp as to filter
   * incoming late partial rankings */
  private var lastTimestampLong = 0L

  /* keeps last sent ranking to avoid outputting duplicates */
  private var lastSentRanking : GenericRankingResult[Long] = new GenericRankingResult[Long]("", List(),10)


  override def processElement(value: GenericRankingResult[Long],
                              ctx: ProcessFunction[GenericRankingResult[Long], GenericRankingResult[Long]]#Context,
                              out: Collector[GenericRankingResult[Long]]): Unit = {


    /* current partial ranking timestamp */
    val timestamp = value.timestamp
    val timestampMillis = Parser.millisFromStringDate(timestamp)


    if(timestampMillis < lastTimestampLong){
      /* clear old partial rankings */
      globalRankingHolder.clearOldRankings(currentTimestamp = timestampMillis)
    } else {
      val globalRanking = globalRankingHolder.globalRanking(partialRanking = value)
      if(notPreviouslySent(globalRanking)){
        /* output global rank computes using current partial ranking */
        out.collect(globalRanking)
        lastSentRanking = globalRanking
      }

      /* update last seen timestamp with current */
      lastTimestampLong = timestampMillis
    }

  }

  /* checks if ranking has already been sent */
  def notPreviouslySent(globalRanking: GenericRankingResult[Long]): Boolean = globalRanking != lastSentRanking
}