package model

import org.joda.time.DateTime

class Post(postId: Long, poster: User, body: String, time: DateTime) {

  var id = postId
  var user = poster
  var content = body
  var timestamp = time


  override def toString = s"Post($id, $user, $content, $timestamp)"
}
