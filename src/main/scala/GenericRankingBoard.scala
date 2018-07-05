import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class GenericRankingBoard[A](k: Int) extends Serializable {

  def this() = this(10)

  var K = k
  private val map : mutable.HashMap[A, Score] = mutable.HashMap()
  //private var topKList :ListBuffer[RankElement] = ListBuffer()

  private object RankElementOrdering extends Ordering[GenericRankElement[A]] {
    override def compare(x: GenericRankElement[A], y: GenericRankElement[A]): Int = {
      x.compare(y)
    }
  }

  private var rankChanged : Boolean = false

  var listBuffer : ListBuffer[GenericRankElement[A]] = ListBuffer()

  def rankHasChanged() : Boolean = {
    if(rankChanged){
      rankChanged = false
      true
    } else false
  }

  def isEmpty() : Boolean = {
    map.isEmpty
  }

  def topK(): List[GenericRankElement[A]] = {
    listBuffer.sortWith(_ >= _).toList
  }

  def printTopK() : Unit = {
    println("Top-K")
    listBuffer.sortWith(_ >= _).foreach(println(_))
  }

  def findListMin() :GenericRankElement[A] = {
    var min = listBuffer.head
    for(el <- listBuffer){
      if(el.score <= min.score){
        min = el
      }
    }
    min
  }

  def updateTop(key: A, value: Score): Unit = {
    if(listBuffer.isEmpty){
      listBuffer += GenericRankElement(key,value)
      rankChanged = true
      return
    }

    val listMin = findListMin()
    if(value < listMin.score && listBuffer.size >= k){ return }

    var toRemove : GenericRankElement[A] = null
    for( el <- listBuffer){
      if(el.id == key){
        toRemove = el
      }
    }

    if(toRemove != null) {
      listBuffer -= toRemove
      listBuffer += GenericRankElement(key,value)
      rankChanged = true
    }

    if(listBuffer.size >= k){
      listBuffer -= findListMin()
      rankChanged = true
    }
    if(toRemove == null) {
      listBuffer += GenericRankElement(key, value)
      rankChanged = true
    }
  }

  def incrementScoreBy(key: A, delta: Score): Unit = {

    if(map.contains(key)){
      val value = map(key).add(delta)
      map += ( key ->  value)
      updateTop(key,value)
    } else {
      map += (key -> delta)
      updateTop(key, delta)
    }
  }

  /*def incrementScore(key: A): Unit = {
    incrementScoreBy(key,1)
  } */

  def size() : Int = {
    map.size
  }

  def scoreOf(key: A) : Int = {
    if(map.contains(key)){
      return map(key).score()
    }
    -1
  }

  def clear(): Unit = {
    map.clear()
    listBuffer.clear()
    rankChanged = true
  }
}
