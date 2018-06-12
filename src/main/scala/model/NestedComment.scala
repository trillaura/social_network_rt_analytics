package model

import org.joda.time.DateTime

class NestedComment(commId: Long, commenter: User, body: String, time: DateTime, parentCommId: Long)
  extends Comment(commId = commId,commenter = commenter, body = body, time = time)  {

  var parentCommID = parentCommId
}
