import java.io.Serializable

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class RankElement[A](id: A, score: Int) extends Ordered[RankElement[A]] with Serializable {
  override def compare(that: RankElement[A]): Int = {this.score.compareTo(that.score) } /* reverse ordering by multiplying by -1 */
  override def toString: String = s"(ID: $id, score: $score)"
}

class RankingBoard[A](k: Int) extends Serializable {
  def this() = this(10)

  var K = k
  private val map : mutable.HashMap[A, Int] = mutable.HashMap()
  //private var topKList :ListBuffer[RankElement] = ListBuffer()

  private object RankElementOrdering extends Ordering[RankElement[A]] {
    override def compare(x: RankElement[A], y: RankElement[A]): Int = {
      x.score.compareTo(y.score)
    }
  }

  private var rankChanged : Boolean = false


  //private var topKTreeSet : mutable.TreeSet[RankElement[A]] = mutable.TreeSet[RankElement[A]]()
  /* keeps min in treeset */

  //private var min : RankElement[A] = null

  var listBuffer : ListBuffer[RankElement[A]] = ListBuffer()

  def merge(otherBoard: RankingBoard[A]) : RankingBoard[A] = {
    var newK = 10
    if(this.K < otherBoard.K){
      newK = this.K
    }else {
      newK = otherBoard.K
    }
    val mergedBoard = new RankingBoard[A](newK)
    val firstTopKBuffer = this.listBuffer.clone()
    val secondTopKBuffer = otherBoard.listBuffer.clone()
    var mergedTopKBuffer = firstTopKBuffer ++: secondTopKBuffer
    mergedTopKBuffer = mergedTopKBuffer.sortWith(_ >= _).slice(0, newK )
    mergedBoard.listBuffer = mergedTopKBuffer
    mergedBoard
  }

  /*def findTreeMin() : RankElement[A] = {
    var min = topKTreeSet.head
    for(el <- topKTreeSet){
      if(el.score <= min.score ){
        min = el
      }
    }
    min
  } */

  /*def updateTopK(key: A, value: Int): Unit = {
    //if(min == null) { min = RankElement(key,value); topKTreeSet += RankElement(key,value); return }
    if(topKTreeSet.isEmpty){
      topKTreeSet.update(RankElement(key, value), true)
      return
    }
    val treeMin = findTreeMin()
    if(value <= treeMin.score){ return } /* no need to update */

    var toRemove :RankElement[A] = null
    for( el <- topKTreeSet){
      if(el.id == key){
        toRemove = el
      }
    }
    if(toRemove != null) {
      topKTreeSet -= toRemove
      topKTreeSet.update( RankElement(key,value), true)
    }

    if(topKTreeSet.size >= k){
      topKTreeSet -= findTreeMin()
    }
    if(toRemove == null) {
      topKTreeSet.update(RankElement(key, value), true)
    }
    //min = topKTreeSet.min
  }
  */

  def rankHasChanged() : Boolean = {
    if(rankChanged){
      rankChanged = false
      true
    } else false
  }

  def isEmpty() : Boolean = {
    map.isEmpty
  }

  def topK(): List[RankElement[A]] = {
    listBuffer.sortWith(_ >= _).toList
  }

  def printTopK() : Unit = {
    println("Top-K")
    //topKTreeSet.foreach(println(_))
    listBuffer.sortWith(_ >= _).foreach(println(_))
  }

  def findListMin():RankElement[A] = {
    var min = listBuffer.head
    for(el <- listBuffer){
      if(el.score <= min.score){
        min = el
      }
    }
    min
  }

  def updateTop(key: A, value: Int): Unit = {
    if(listBuffer.isEmpty){
      listBuffer += RankElement(key,value)
      rankChanged = true
      return
    }

    val listMin = findListMin()
    if(value < listMin.score && listBuffer.size >= k){ return }

    var toRemove :RankElement[A] = null
    for( el <- listBuffer){
      if(el.id == key){
        toRemove = el
      }
    }

    if(toRemove != null) {
      listBuffer -= toRemove
      listBuffer += RankElement(key,value)
      rankChanged = true
    }

    if(listBuffer.size >= k){
      listBuffer -= findListMin()
      rankChanged = true
    }
    if(toRemove == null) {
      listBuffer += RankElement(key, value)
      rankChanged = true
    }
  }

  def incrementScoreBy(key: A, delta: Int): Unit = {
    var value = map.getOrElse(key, -1)
    if(value == -1){
      value += 1 + delta
      //map(key) = delta
    } else {
      value += delta
    }
    map += (key -> value)

    //updateTopK(key,value)
    updateTop(key,value)
  }

  def incrementScore(key: A): Unit = {
    incrementScoreBy(key,1)
  }

  def size() : Int = {
    map.size
  }

  def scoreOf(key: A) : Int = {
    map.getOrElse(key, -1)
  }

  def clear(): Unit = {
    map.clear()
    listBuffer.clear()
    rankChanged = true
    //topKTreeSet.clear()
    //min = null
  }

}
