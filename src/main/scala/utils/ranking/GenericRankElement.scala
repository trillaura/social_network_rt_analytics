package utils.ranking

import java.io.Serializable

case class GenericRankElement[A](id: A, score: Score) extends Ordered[GenericRankElement[A]] with Serializable {
  override def compare(that: GenericRankElement[A]): Int = {this.score.compareTo(that.score) } /* reverse ordering by multiplying by -1 */
  override def toString: String = s"(ID: $id, score: $score)"
  def merge(that : GenericRankElement[A]) :GenericRankElement[A] = {
    if(this.id != that.id){ println("Warning! Merging two RankElements with different IDs. Using first ID")}
    GenericRankElement[A](this.id, this.score.add(that.score))
  }
}
