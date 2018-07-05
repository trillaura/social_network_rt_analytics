import utils.Parser

import scala.collection.immutable.TreeMap


/**
  * Holds partial rankings with relative timestamps
  * and computes global ranking
  * @param diff
  * @tparam A
  */
class GlobalRankingHolder[A](diff: Long) extends Serializable {

  /* defines for how much time to keep partial rankings */
  var delta = diff

  /* keeps the mapping between timestamp (ordered) and ranking */
  private var treeMap : TreeMap[Long, GenericRankingResult[A]] = TreeMap.empty[Long, GenericRankingResult[A]]

  /**
    * clears rankings older than current - delta to
    * save space
    * @param currentTimestamp
    */
  def clearOldRankings(currentTimestamp: Long): Unit ={
    if(treeMap.nonEmpty) {
      treeMap = treeMap.tail.dropWhile(el => currentTimestamp - el._1 > delta)   /* drops rankings older than delta milliseconds (no needed anymore) */
    }
  }

  /**
    * given a new partial ranking, tries to find
    * other partial ranking with same timestamp,
    * merges them and returns global ranking.
    * If no partial ranking is present with same
    * timestamp, returns current partial ranking
    * @param partialRanking
    * @return global ranking with timestamp of partialRanking
    */
  def globalRanking(partialRanking: GenericRankingResult[A]):  GenericRankingResult[A] = {
    val timestampMillis = Parser.millisFromStringDate(partialRanking.timestamp)
    if(!treeMap.contains(timestampMillis)){
      treeMap += (timestampMillis -> partialRanking)                            /* new ranking, no previous one present    */
      partialRanking
    } else {
      val previousRanking = treeMap(timestampMillis)                            /* get previous ranking                    */
      val mergedRank = previousRanking.mergeRank(partialRanking)                /* merge it with new one                   */
      treeMap += (timestampMillis -> mergedRank)                                /* update it in the treemap                */
      mergedRank
    }
  }
}
