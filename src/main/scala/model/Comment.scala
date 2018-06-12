package model

import org.joda.time.DateTime

abstract class Comment(commId: Long, commenter: User, body: String, time: DateTime) {

  var id = commId
  var user = commenter
  var content = body
  var timestamp = time
}
