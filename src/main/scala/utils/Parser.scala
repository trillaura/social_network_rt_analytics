package utils

import model.{User, UserConnection}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.io.Source

object Parser {

  /* NOTE: needs to be a char. Single ' */
  val PIPE_DELIMITER = '|'

  private lazy val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ")

  def readFile(path: String) : Unit = {

    for(l <- Source.fromFile(path).getLines()){
      println(parseUserConnection(l))
    }
  }

  def parseUserConnection(line: String) : Option[UserConnection] = {
    val cols = split(line)
    if(cols.length != 3)
      None

    try {
      val timestampString = cols(0)
      val firstUser = new User(cols(1).toLong)
      val secondUser = new User(cols(2).toLong)

      Some(new UserConnection(convertToDateTime(timestampString).toDateTime(DateTimeZone.forID("Etc/GMT-1")), firstUser, secondUser))
    } catch {
      case ex : Exception =>  ex.printStackTrace(); None
    }
  }

  def convertToDateTime(timeS: String) : DateTime = {
      dateFormatter.parseDateTime(timeS.replace("T", " "))
  }

  def split(line: String): Array[String] = {
    line.split(PIPE_DELIMITER).map(_.trim)
  }
}
