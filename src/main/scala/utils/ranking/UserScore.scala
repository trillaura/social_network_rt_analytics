package utils.ranking

import java.io.Serializable

/**
  * User score made of three parts
  * @param a # created friendships
  * @param b # posts created
  * @param c # comments created
  */
case class UserScore(a: Int, b : Int, c: Int) extends Score with Serializable{
  override def add(other: Any): Score = {
    if(!other.isInstanceOf[UserScore]){ println("Trying to add two scores with different types"); return this; }
    val otherScore = other.asInstanceOf[UserScore]
    UserScore(a + otherScore.a, b + otherScore.b, c + otherScore.c)
  }

  override def compare(that: Any): Int = {
    if(!that.isInstanceOf[UserScore]){ println("Trying to add two scores with different types"); return 0; }
    val thatScore = that.asInstanceOf[UserScore]
    this.score().compareTo(thatScore.score())
  }

  override def toString: String = s"(a:$a,b:$b,c:$c)"

  override def score(): Int = {a + b + c}
}
