package utils.ranking

import utils.ranking

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import util.control.Breaks._

/**
  * Data structure that maintains and updates current score
  * of ranked elements and caches and updates the top-K
  * elements so to retrieve them in constant time
  * @param k number of top elements to store
  * @tparam A type of ranking element key
  */
class GenericRankingBoard[A](k: Int) extends Serializable {

  def this() = this(10)   /* default k = 10*/

  var K = k

  /* keeps updated score of rank elements */
  private val map : mutable.HashMap[A, Score] = mutable.HashMap()
  /* keeps updated top-k elements in the hashmap */
  private var listBuffer : ListBuffer[GenericRankElement[A]] = ListBuffer()

  /* indicates whether the rank has changed from previous check */
  private var rankChanged : Boolean = false

  /**
    * @return true if rank has changed since
    *         last check
    */
  def rankHasChanged() : Boolean = {
    if(rankChanged){
      rankChanged = false
      true
    } else false
  }

  /**
    * Board has no elements
    * @return
    */
  def isEmpty() : Boolean = {
    map.isEmpty
  }

  /**
    * @return sorted list of top-k elements
    */
  def topK(): List[GenericRankElement[A]] = {
    listBuffer.sortWith(_ >= _).toList
  }

  def printTopK() : Unit = {
    println(s"Top-$K")
    listBuffer.sortWith(_ >= _).foreach(println(_))
  }

  /**
    * @return element with min score in buffered top-k
    */
  private def findListMin() : GenericRankElement[A] = {
    var min = listBuffer.head
    for(el <- listBuffer){
      if(el.score <= min.score){
        min = el
      }
    }
    min
  }

  /**
    * add an element (not in order) to top k
    * sets the rank as changed
    * @param key ID
    * @param value score
    */
  private def addElementTopK(key: A, value: Score) = {
    listBuffer += ranking.GenericRankElement(key,value)
    rankChanged = true
  }

  /**
    * finds and removes minimun from top-k
    * to leave space for other elements with
    * higher score than minimum;
    * sets the rank as changed
    */
  private def removeMinFromTopK(): Unit = {
    listBuffer -= findListMin()
    rankChanged = true
  }

  /**
    * updates top-k and sets the rank changed status
    * accordingly
    *
    * @param key ID
    * @param value total score
    */
  private def updateTop(key: A, value: Score): Unit = {
    if(listBuffer.isEmpty){
      addElementTopK(key,value)
      return
    }

    val listMin = findListMin()
    if(value < listMin.score && listBuffer.size >= k){ return }   /* if new score is less than minimum and
                                                                    list buffer is full, don't change anything */

    breakable {
      for( el <- listBuffer){                                     /* checks if there is another element with same ID
                                                                     and eliminates it by adding new one */
        if(el.id == key){
          listBuffer -= el
          addElementTopK(key,value)
          break                                                   /* there could be at most 1 element with same ID */
        }
      }
      addElementTopK(key,value)                                   /* Note: executed only if break didn't execute */
    }

    while (listBuffer.size > k){
      removeMinFromTopK()                                         /* remove elements with minimum score until top-k has
                                                                     exactly k elements or less */
    }
  }

  /**
    * Increments the score of the rank element with ID key
    * and updates (if needed) the cached top-k
    * @param key ID
    * @param delta difference to add to current score
    */
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

  /**
    * @return num total rank elements
    */
  def size() : Int = {
    map.size
  }

  /**
    * @param key ID
    * @return unified score of element
    *         of key ID
    */
  def scoreOf(key: A) : Int = {
    if(map.contains(key)){
      return map(key).score()
    }
    -1
  }

  /**
    * Clears all data structures
    * and sets the rank to changed
    * state
    */
  def clear(): Unit = {
    map.clear()
    listBuffer.clear()
    rankChanged = true
  }
}
