import org.scalatest.FlatSpec
import utils.Parser

class ParserTestSuite extends FlatSpec {

  "The Parser object " should "parse correct timestamp format" in {

    var testTimeStamp = "2010-02-03T16:35:50.015+0000"
    val parsedTimeStamp = Parser.convertToDateTime(testTimeStamp)

    assert(parsedTimeStamp.getYear == 2010)
    assert(parsedTimeStamp.getMonthOfYear == 2)
    assert(parsedTimeStamp.getDayOfMonth == 3)
    assert(parsedTimeStamp.getHourOfDay == 16)
    assert(parsedTimeStamp.getMinuteOfHour == 35)
    assert(parsedTimeStamp.getSecondOfMinute == 50)
  }

}
