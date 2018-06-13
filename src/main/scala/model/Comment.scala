package model

import org.joda.time.DateTime

class Comment(commId: Long, commenter: User, body: String, time: DateTime, commentToPost: Boolean, parent: Long) {

  var id = commId
  var user = commenter
  var content = body
  var timestamp = time
  var postComment = commentToPost
  var parentID = parent

  def isPostComment(): Boolean = {
    postComment
  }

  def isCommentToComment() : Boolean = {
    !postComment
  }


  override def toString = s"Comment($id, $user, $content, $timestamp, $postComment, $parentID)"
}
