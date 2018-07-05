import org.joda.time.DateTime
import utils.ranking.GenericRankElement

import scala.collection.mutable.ListBuffer

class GenericRankingResult[A](time: String, elements : List[GenericRankElement[A]], k: Int) extends Serializable {
  var timestamp : String = time
  var rankElements : List[GenericRankElement[A]] = elements
  var K : Int = k

  def mergeRank(otherRankingResult : GenericRankingResult[A]) : GenericRankingResult[A] = {
    var finalTimestamp = ""
    var finalK = 0
    val thisMillis = new DateTime(this.timestamp).getMillis
    val thatMillis = new DateTime(otherRankingResult.timestamp).getMillis
    if(thisMillis != thatMillis) {
      //println("WARNING! merging two rankings with different timestamps")
      if (thisMillis > thatMillis) {
        //finalTimestamp = this.timestamp
        finalTimestamp = otherRankingResult.timestamp
      } else {
        //finalTimestamp = otherRankingResult.timestamp
        finalTimestamp = this.timestamp
      }
    }else {
      finalTimestamp = this.timestamp
    }

    if(this.K < otherRankingResult.K){
      finalK = this.K
    } else {
      finalK = otherRankingResult.K
    }

    var listBuffer: ListBuffer[GenericRankElement[A]] = ListBuffer()
    this.rankElements.foreach(el => listBuffer += el)
    otherRankingResult.rankElements.foreach(el => listBuffer += el)

    val finalElements = listBuffer.distinct.sortWith(_ >= _).slice(0, finalK).toList
    new GenericRankingResult[A](finalTimestamp, finalElements, finalK)

  }

  override def toString = s"($timestamp, $rankElements, $K)"
}
