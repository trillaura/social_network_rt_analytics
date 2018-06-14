import org.scalatest.FlatSpec
import utils.Parser

class ParserTestSuite extends FlatSpec {

  "The Parser object " should "parse correct timestamp format" in {

    var testTimeStamp = "2010-02-03T22:35:50.015+0000"
    val parsedTimeStamp = Parser.convertToDateTime(testTimeStamp)


    assert(parsedTimeStamp.getYear == 2010)
    assert(parsedTimeStamp.getMonthOfYear == 2)
    assert(parsedTimeStamp.getDayOfMonth == 3)
    assert(parsedTimeStamp.getHourOfDay == 22)
    assert(parsedTimeStamp.getMinuteOfHour == 35)
    assert(parsedTimeStamp.getSecondOfMinute == 50)
    assert(parsedTimeStamp.getMillisOfSecond == 15)
  }

  it should "create correct post comment" in {
    val commString = "2010-02-09T04:05:20.777+0000|529590|2886|LOL|Baoping Wu||529360"

    val actualComment = Parser.parseComment(commString)
    assert(actualComment.isDefined)

    assert(actualComment.get.id == 529590)
    assert(actualComment.get.user.name == "Baoping Wu")
    assert(actualComment.get.user.id == 2886)
    assert(actualComment.get.content == "LOL")
    assert(actualComment.get.postComment)
    assert(actualComment.get.parentID == 529360)

  }


  it should "create correct comment to comment" in {
    val commString = "2010-02-09T07:19:44.946+0000|529595|2886|good|Baoping Wu|529592|"

    val actualComment = Parser.parseComment(commString)
    assert(actualComment.isDefined)

    assert(actualComment.get.id == 529595)
    assert(actualComment.get.user.name == "Baoping Wu")
    assert(actualComment.get.user.id == 2886)
    assert(actualComment.get.content == "good")
    assert(!actualComment.get.postComment)
    assert(actualComment.get.parentID == 529592)

  }

  it should "not create malformed comment" in {
    val commString = "2010-02-09T07:19:44.946+0000|529595|wrong|good|Baoping Wu|529592|"

    val actualComment = Parser.parseComment(commString)
    assert(actualComment.isEmpty)

  }

  it should "not create comment with negative values" in {
    val commString = "2010-02-16T06:51:56.345+0000|-571692|1259|duh|Mee Vongvichit||571477"
    val actualComment = Parser.parseComment(commString)
    assert(actualComment.isEmpty)
  }

  it should "create correct post from line" in {
    val postString = "2010-02-02T19:53:43.226+0000|299101|4661|photo299101.jpg|Michael Wang"

    val post = Parser.parsePost(postString)
    assert(post.isDefined)

    assert(post.get.id == 299101)
    assert(post.get.user.id == 4661)
    assert(post.get.user.name == "Michael Wang")
    assert(post.get.content == "photo299101.jpg")

  }

}
