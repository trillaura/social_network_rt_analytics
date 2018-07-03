package utils

import model.{Comment, Post, User, UserConnection}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Parser {

  /* NOTE: needs to be a char. Single ' */
  val PIPE_DELIMITER = '|'

  val DEFAULT_DATETIME_ZONE = "Europe/Dublin"
  val TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
  private lazy val dateFormatter = DateTimeFormat.forPattern(TIMESTAMP_FORMAT)

  def readFile(path: String) : Unit = {

    for(l <- Source.fromFile(path).getLines()){
      val parsed = parseComment(l)
      if(parsed.isEmpty){
        println(l)
      }
    }
  }

  def fileLinesAsList(path: String) : ListBuffer[String] = {

    var list : ListBuffer[String] = ListBuffer()
    for(l <- Source.fromFile(path).getLines()){
      list += l
    }

    list

  }

  def parsePost(line: String) : Option[Post] = {
    val cols = split(line)
    if(cols.length != 5){ return None }

    try {
      val timestampString = cols(0)
      val postID = cols(1).toLong
      val userID = cols(2).toLong
      val content = cols(3)
      val username = cols(4)


      /* check integrity. Post CAN be EMPTY */
      if( postID < 0 || userID < 0 || username.isEmpty ){ return None }

      val user = new User(userID, username)

      Some(new Post(postID, user, content, convertToDateTime(timestampString)))

    } catch {
      case ex : Exception =>  ex.printStackTrace(); None
    }
  }

  def parseUserConnection(line: String) : Option[UserConnection] = {
    val cols = split(line)
    if(cols.length != 3){ return None }

    try {
      val timestampString = cols(0)
      val firstUserID = cols(1).toLong
      val secondUserID = cols(2).toLong

      /* check integrity */
      if(firstUserID < 0 ||  secondUserID < 0) {
        return None
      }

      val firstUser = new User(firstUserID)
      val secondUser = new User(secondUserID)

      val timestamp = convertToDateTime(timestampString)
      Some(new UserConnection(timestamp, firstUser, secondUser))
    } catch {
      case ex : Exception =>  ex.printStackTrace(); None
    }
  }

  def parseComment(line: String) : Option[Comment] = {

    var postComment = true
    val cols = split(line)

    /* not post comment nor comment of comment */
    if(cols.length != 6 && cols.length != 7){ return None }

    /* split function returns 6 fields for comment of comment
    * and 7 for post comment */
    if(cols.length == 6) { postComment = false }

    try {
      val timestampString = cols(0)
      val commentID = cols(1).toLong
      val userID = cols(2).toLong
      val content = cols(3)
      val username = cols(4)

      var parentID = 0L
      if(postComment) {
        parentID = cols(6).toLong
      } else {
        parentID = cols(5).toLong
      }

      /* check integrity */
      if( commentID < 0 || userID < 0 || content.isEmpty || username.isEmpty || parentID < 0){ return None }

      val user = new User(userID, username)

      Some(new Comment(commentID, user, content, convertToDateTime(timestampString), postComment, parentID))

    } catch {
      case ex : Exception =>  ex.printStackTrace(); None
    }
  }

  def convertToDateTime(timeS: String) : DateTime = {
      dateFormatter.parseDateTime(timeS).withZone(DateTimeZone.UTC)
  }

  def split(line: String): Array[String] = {
    line.split(PIPE_DELIMITER).map(_.trim)
  }
}
