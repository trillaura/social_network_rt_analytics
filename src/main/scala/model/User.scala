package model

class User(userId: Long, userName: String) {

  def this(userId: Long) = this(userId,"")

  var id = userId
  var name = userName

}
