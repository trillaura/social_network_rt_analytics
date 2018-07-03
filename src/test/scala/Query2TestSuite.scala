import java.util
import java.util.Comparator

import model.{Comment, User}
import org.joda.time.DateTime
import org.scalatest.FlatSpec
import utils.Parser

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Query2TestSuite extends FlatSpec {

  val ordering = Ordering[Int].on[(Long, Int)](_._2)

  "Hash map" should "do fine" in {

    var map = mutable.HashMap[Long, Int]()

    time {
      val max = 100000
      for (i <- 0 until max) {
        map += ((i.toLong, i))
        //tm.add( (i.toLong, i ))
        //tm.update((i.toLong, i), true)

        var value = map.getOrElse(i.toLong, -1)
        value += 1
        map.update(i.toLong, value)

      }

      assert(map.size == max)
    }

    println(map.max)

  }

  object MOrd extends Ordering[(Long,Int)]{
    override def compare(x: (Long, Int), y: (Long, Int)): Int = {
      x._2.compareTo(y._2)
    }
  }

  "Treesets" should "insert all items " in {

    var tm : mutable.TreeSet[(Long, Int)] = mutable.TreeSet()(MOrd.reverse)

    time {
      val max = 100000
      for (i <- 0 until max) {
        tm += ((i.toLong, i))
        //tm.add( (i.toLong, i ))
        tm.update((i.toLong, i), true)

      }

      assert(tm.size == max)
    }

    println(tm.head)
  }


  object MyC extends Comparator[(Long, Int)]{
    override def compare(o1: (Long, Int), o2: (Long, Int)): Int = {
      o1._2.compareTo(o2._2)
    }
  }


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000 / 1000+ "s")
    result
  }

  "Query 2" should  "find posts with maximum # of comments" in {

    var lines = Parser.fileLinesAsList("dataset/comments.dat")

    var map : Map[Long, Int] = Map()

    var comment = new Comment(0l,new User(0L), "", new DateTime(1),true,1L)
    var count = 0
    for( line <- lines ){
      comment = Parser.parseComment(line = line).get
      if(comment.isPostComment()) {
        count = map getOrElse(comment.parentID, 0)
        count += 1
        map += (comment.parentID -> count)
      }
    }

    var finalK : ListBuffer[(Long, Int)] = ListBuffer()
    //val listMap = ListMap(map.toSeq.sortBy(_._2):_*)

    //println(map getOrElse (34361155726L, 0))

    var lastMaxIDs : ListBuffer[Long] = ListBuffer()
    var max = (Long.MaxValue , Int.MaxValue)
    for(i <- 0 until 10){
      max = map.filter(_._2 <= max._2).filter( el => !lastMaxIDs.contains(el._1)).max(ordering)
      lastMaxIDs += max._1
      finalK += max
    }

    finalK.map(println(_))

  }
}
