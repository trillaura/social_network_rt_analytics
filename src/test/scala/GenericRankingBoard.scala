import org.scalatest.FlatSpec
import utils.ranking.{GenericRankingBoard, SimpleScore}

class GenericRankingBoardTestSuite extends FlatSpec {


  val rankingBoard : GenericRankingBoard[Long] = new GenericRankingBoard[Long]

  "The Generic utils.ranking.RankingBoard" should "insert elements correctly" in {

    rankingBoard.clear()

    rankingBoard.incrementScoreBy(1, SimpleScore(1))
    rankingBoard.incrementScoreBy(1, SimpleScore(1))

    rankingBoard.incrementScoreBy(10, SimpleScore(2))
    rankingBoard.incrementScoreBy(10, SimpleScore(3))

    assert(rankingBoard.size() == 2)
    assert(rankingBoard.scoreOf(1) == 2)
    assert(rankingBoard.scoreOf(10) ==  5)
    assert(rankingBoard.scoreOf(20) ==  -1)

    rankingBoard.printTopK()

  }

  it should "find the Top-K correctly" in {

    rankingBoard.clear()

    rankingBoard.incrementScoreBy(1, SimpleScore(1))
    rankingBoard.incrementScoreBy(1,SimpleScore(1))
    rankingBoard.incrementScoreBy(2, SimpleScore(10))
    rankingBoard.incrementScoreBy(3,SimpleScore(5))
    rankingBoard.incrementScoreBy(4, SimpleScore(1))
    rankingBoard.incrementScoreBy(5, SimpleScore(1))
    rankingBoard.incrementScoreBy(6,SimpleScore(9))
    rankingBoard.incrementScoreBy(7,SimpleScore(2))
    rankingBoard.incrementScoreBy(8,SimpleScore(2))
    rankingBoard.incrementScoreBy(9, SimpleScore(22))
    rankingBoard.incrementScoreBy(10, SimpleScore(101))
    rankingBoard.incrementScoreBy(11, SimpleScore(99))

    rankingBoard.printTopK()

  }
}
