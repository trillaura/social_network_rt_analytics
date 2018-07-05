package utils.ranking

import java.io.Serializable

case class RankElement[A](id: A, score: Int) extends Ordered[RankElement[A]] with Serializable {
  override def compare(that: RankElement[A]): Int = {this.score.compareTo(that.score) } /* reverse ordering by multiplying by -1 */
  override def toString: String = s"(ID: $id, score: $score)"
  def merge(that : RankElement[A]) :RankElement[A] = {
    if(this.id != that.id){ println("Warning! Merging two RankElements with different IDs. Using first ID")}
    RankElement[A](this.id, this.score + that.score)
  }
}
