class RankingResult[A](time: String, elements : List[RankElement[A]]) {
  var timestamp : String = time
  var rankElements : List[RankElement[A]] = elements


  override def toString = s"Result($timestamp, $rankElements)"
}
