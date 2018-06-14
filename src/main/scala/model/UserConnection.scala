package model

import org.joda.time.DateTime

class UserConnection(time: DateTime, user1: User, user2: User) {

  var timestamp = time
  var firstUser = user1
  var secondUser = user2


  override def toString = s"UserConnection($timestamp, $firstUser, $secondUser)"
}
