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
    assert(rankingBoard.scoreOf(1) == 3)
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



}
