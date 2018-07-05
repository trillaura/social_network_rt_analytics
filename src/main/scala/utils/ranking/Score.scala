package utils.ranking

/**
  * Used to rank items in Ranking board
  * and Ranking Results
  */
trait Score extends Ordered[Any] {
  /**
    * sum two scores
    * @param other
    * @return
    */
  def add(other : Any) : Score

  /**
    * defines a unified integer score to
    * compare to others. E.g. if the class
    * uses a tuple to rank, the score could
    * be the sum of its components
    * @return
    */
  def score(): Int
}
