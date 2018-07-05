package utils.ranking

trait Score extends Ordered[Any] {
  def add(other : Any) : Score
  def score(): Int
}
