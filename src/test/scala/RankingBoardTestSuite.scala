import org.scalatest.FlatSpec

class RankingBoardTestSuite extends FlatSpec {

  val rankingBoard : RankingBoard[Long] = new RankingBoard[Long]

  "The RankingBoard" should "insert elements corretly" in {

    rankingBoard.clear()

    rankingBoard.incrementScore(1)
    rankingBoard.incrementScore(1)
    rankingBoard.incrementScore(1)

    rankingBoard.incrementScore(10)
    rankingBoard.incrementScore(10)

    assert(rankingBoard.size() == 2)
    assert(rankingBoard.scoreOf(1) == 2)
    assert(rankingBoard.scoreOf(10) == 2)

    rankingBoard.printTopK()
  }

  it should "handle high input rate" in {

    rankingBoard.clear()

    val maxIterations = 100000
    for( i <- 0 until maxIterations){
      rankingBoard.incrementScore(i.toLong)
    }

    assert(rankingBoard.size() == maxIterations)
  }

  it should "increment score with delta" in {

    rankingBoard.clear()

    rankingBoard.incrementScoreBy(2,10)

    assert(rankingBoard.scoreOf(2) == 10)

    rankingBoard.incrementScoreBy(2,10)

    assert(rankingBoard.scoreOf(2) == 20)

    rankingBoard.incrementScore(2)

    assert(rankingBoard.scoreOf(2) == 21)

    rankingBoard.printTopK()
  }

  it should "find the Top-K correctly" in {

    rankingBoard.clear()

    rankingBoard.incrementScore(1)
    rankingBoard.incrementScore(1)
    rankingBoard.incrementScoreBy(2, 10)
    rankingBoard.incrementScoreBy(3,5)
    rankingBoard.incrementScore(4)
    rankingBoard.incrementScore(5)
    rankingBoard.incrementScoreBy(6,9)
    rankingBoard.incrementScoreBy(7,2)
    rankingBoard.incrementScoreBy(8,2)
    rankingBoard.incrementScoreBy(9, 22)
    rankingBoard.incrementScoreBy(10, 101)
    rankingBoard.incrementScoreBy(11, 99)

    rankingBoard.printTopK()

  }

  it should "insert values with same score with the most recent ones as most rated" in {

    rankingBoard.clear()

    rankingBoard.incrementScore(1)
    rankingBoard.incrementScore(2)
    rankingBoard.incrementScore(3)
    rankingBoard.incrementScore(4)
    rankingBoard.incrementScore(5)

    val topK = rankingBoard.topK()
    assert(topK(0).id == 5)
    assert(topK(1).id == 4)
    assert(topK(2).id == 3)
    assert(topK(3).id == 2)
    assert(topK(4).id == 1)
  }

  it should "merge correctly with other board" in {

    val firstRankingBoard = new RankingBoard[Long](5)
    val secondRankingBoard = new RankingBoard[Long](5)


    firstRankingBoard.incrementScore(1)
    firstRankingBoard.incrementScore(1)
    firstRankingBoard.incrementScoreBy(2, 10)
    firstRankingBoard.incrementScoreBy(3,5)
    firstRankingBoard.incrementScore(4)
    firstRankingBoard.incrementScore(5)

    secondRankingBoard.incrementScoreBy(6,100)
    secondRankingBoard.incrementScoreBy(7, 999)
    secondRankingBoard.incrementScoreBy(8,11)
    secondRankingBoard.incrementScoreBy(9,6)
    secondRankingBoard.incrementScoreBy(10,4)

    val mergedRankingBoard = firstRankingBoard.merge(secondRankingBoard)
    mergedRankingBoard.printTopK()

  }

  "The Generic Ranking Element " should " add Simple Score values correctly" in {

    val score1 : SimpleScore = SimpleScore(1)
    val score2 : SimpleScore = SimpleScore(3)

    val rankElement1 = GenericRankElement[Long](10, score1)
    val rankElement2 = GenericRankElement[Long](10, score2)
    val finalRankElement = rankElement1.merge(rankElement2)
    println(finalRankElement)

    val simpleScore = finalRankElement.score.asInstanceOf[SimpleScore]
    assert(simpleScore.value == 4)
  }

  it should "compare Simple score correctly" in {
    val score1 : SimpleScore = SimpleScore(1)
    val score2 : SimpleScore = SimpleScore(3)
    val score3 : SimpleScore = SimpleScore(3)

    assert(score1.compare(score2) < 0)
    assert(score2.compare(score1) > 0)
    assert(score2.compare(score3) == 0)
  }

  it should " add User Score values correctly" in {
    val score1 = UserScore(1,2,3)
    val score2 = UserScore(3,4,5)

    println("User score comparison " + score1.compare(score2))

    val rankElement1 = GenericRankElement[Long](10, score1)
    val rankElement2 = GenericRankElement[Long](10, score2)
    val finalRankElement = rankElement1.merge(rankElement2)
    println(finalRankElement)

    val userScore = finalRankElement.score.asInstanceOf[UserScore]
    assert(userScore.a == 4)
    assert(userScore.b == 6)
    assert(userScore.c == 8)


  }

  it should "compare User Score correctly" in {
    val score1 = UserScore(1,2,3) /* score 6 */
    val score2 = UserScore(3,4,5) /* score 12 */
    val score3 = UserScore(2,2,2) /* score 6 */

    assert(score1.compare(score2) < 0)
    assert(score2.compare(score1) > 0)
    assert(score1.compare(score3) == 0)
    assert(score3.compare(score1) == 0)
  }



}
