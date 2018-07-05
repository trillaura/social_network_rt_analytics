package utils.ranking

import utils.Parser

import scala.collection.mutable.ListBuffer

/**
  * Contains a ranking of Rank elements, a given statistic
  * start time and k (max number of elements in the rank)
  * Note that the list can have less elements than k
  * @param startTime statistic start time
  * @param elements  rank element with relative ID and score
  * @param k max rank elements
  * @tparam A type of rank elements ID
  */
class GenericRankingResult[A](startTime: String, elements : List[GenericRankElement[A]], k: Int) extends Serializable {

  var timestamp : String = startTime
  var rankElements : List[GenericRankElement[A]] = elements
  var K : Int = k  /* useful when merging two ranks */

  /**
    * Merges this rank with another one.
    * The new Rank has:
    *    - finalTimestamp = min(thisTimestamp, thatTimestamp)
    *    - finalK = min(thisK, thatK)
    *    - finalRankedElements = (thisElement + thatElements).top(finalK)
    * @param otherRankingResult
    * @return merged results
    */
  def mergeRank(otherRankingResult : GenericRankingResult[A]) : GenericRankingResult[A] = {
    val thisMillis = Parser.millisFromStringDate(this.timestamp)
    val thatMillis = Parser.millisFromStringDate(otherRankingResult.timestamp)

    val finalK = if(this.K < otherRankingResult.K)  this.K  else otherRankingResult.K
    val finalTimestamp = if(thisMillis != thatMillis && thisMillis > thatMillis) otherRankingResult.timestamp else this.timestamp

    new GenericRankingResult[A](finalTimestamp, finalMergedRanking(otherRankingResult, finalK), finalK)

  }


  /**
    * Merges two lists of ranked elements into a list
    * with only the top finalK elements present
    * @param otherRankingResult
    * @param finalK
    * @return
    */
  private def finalMergedRanking(otherRankingResult: GenericRankingResult[A],
                                 finalK : Int): List[GenericRankElement[A]] = {

    var listBuffer: ListBuffer[GenericRankElement[A]] = ListBuffer()
    this.rankElements.foreach(el => listBuffer += el)
    otherRankingResult.rankElements.foreach(el => listBuffer += el)
    listBuffer.distinct.sortWith(_ >= _).slice(0, finalK).toList
  }

  override def toString = s"($timestamp, $rankElements, $K)"
}
