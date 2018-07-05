package utils.ranking

case class SimpleScore( value : Int) extends Score {
  override def add(other: Any): Score = {
    if(!other.isInstanceOf[SimpleScore]){ println("Trying to add two scores with different types"); return this; }
    val otherScore = other.asInstanceOf[SimpleScore]
    SimpleScore(this.value + otherScore.value)
  }

  override def compare(that: Any): Int = {
    if(!that.isInstanceOf[SimpleScore]){ println("Trying to add two scores with different types"); return 0; }
    val otherScore = that.asInstanceOf[SimpleScore]
    this.score().compareTo(otherScore.score())
  }

  override def toString: String = s"$value"

  override def score(): Int = this.value
}
