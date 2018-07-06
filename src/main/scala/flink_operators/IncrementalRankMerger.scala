package flink_operators

import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import utils.ranking.GenericRankingResult

import scala.collection.mutable

/**
  * Merges rankings for current time window based on rankings of
  * previous smaller time windows. E.g. computes daily rankings
  * using hourly rankings
  */
class IncrementalRankMerger extends ProcessAllWindowFunction[GenericRankingResult[Long],GenericRankingResult[Long],TimeWindow] {

  /* keeps the starting timestamp of last window */
  var lastWindowStart = 0L

  /* keeps the ranking associated with a given timestamp */
  var hashMap : mutable.HashMap[String, GenericRankingResult[Long]] = mutable.HashMap()

  override def process(context: Context, elements: Iterable[GenericRankingResult[Long]], out: Collector[GenericRankingResult[Long]]): Unit = {
    val currWindow = context.window

    updateState(currWindow)
    updateHashMap(elements)
    out.collect(computeFinalRank())
  }

  /**
    * updates new window timestamp
    * and clears hashmap if we are in a new
    * time window
    * @param currWindow
    */
  def updateState(currWindow: TimeWindow) = {
    if(currWindow.getStart > lastWindowStart){
      lastWindowStart = currWindow.getStart
      hashMap.clear()
      /*out.collect(new GenericRankingResult[Long]("============= NEW WINDOW with start %s and duration for %d ============= "
        .format(Parser.convertToDateString(currWindow.getStart), (currWindow.getEnd - currWindow.getStart) / 1000 / 60), List(), 0))*/
    }
  }

  /**
    * Updates hashmap by merging incoming ranks with
    * older ones (if present)
    * @param elements
    */
  def updateHashMap(elements: Iterable[GenericRankingResult[Long]]) = {
    elements.foreach(el => {
      if(hashMap.contains(el.timestamp)){
        val got = hashMap(el.timestamp)
        hashMap += (el.timestamp -> got.mergeRank(el))
      } else {
        hashMap += (el.timestamp -> el)
      }
    })
  }

  /**
    * Incrementally merges ranks to have a final one
    * for that particular time window as the union
    * of ranks in smaller time windows within the
    * current larger window
    * @return
    */
  def computeFinalRank(): GenericRankingResult[Long] = {
    var finalIncrementalRank : GenericRankingResult[Long] = null
    for( (k,v) <- hashMap){
      if(finalIncrementalRank == null){
        finalIncrementalRank = v
      } else {
        finalIncrementalRank = finalIncrementalRank.incrementalMerge(v)
      }
    }
    finalIncrementalRank
  }


}