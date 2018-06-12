package model

import org.joda.time.DateTime

class PostComment(commId: Long, commenter: User, body: String, time: DateTime, postId: Long)
  extends Comment(commId = commId,commenter = commenter, body = body, time = time) {

  var postID = postId
}
