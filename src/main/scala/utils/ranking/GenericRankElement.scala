package utils.ranking

import java.io.Serializable

/**
  * Represents an item in a rank
  * @param id item identification (can be any type)
  * @param score its score to be compared to others (SimpleScore, UserScore, ecc)
  * @tparam A type of the ID
  */
case class GenericRankElement[A](id: A, score: Score) extends Ordered[GenericRankElement[A]] with Serializable {

  override def compare(that: GenericRankElement[A]): Int = {this.score.compareTo(that.score) } /* reverse ordering by multiplying by -1 */

  /**
    * Given two rank elements, merges them into a
    * final rank element with same id and
    * the sum of the scores (defined in every implementation)
    * @param that other rank element to be merged
    * @return new merged rank element
    */
  def merge(that : GenericRankElement[A]) :GenericRankElement[A] = {
    if(this.id != that.id){ println("Warning! Merging two RankElements with different IDs. Using first ID")}
    GenericRankElement[A](this.id, this.score.add(that.score))
  }

  override def toString: String = s"(ID: $id, score: $score)"
}
