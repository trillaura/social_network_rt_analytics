package storm.Bolt

object Config {

  /* useful parameters for the WindowCountBolt */
  val TOPOLOGY_BOLTS_WINDOW_SIZE_MS: String = "topology.bolts.window.size.ms"
  val TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS: String = "topology.bolts.sliding.interval.duration.ms"
  val TOPOLOGY_BOLTS_WINDOW_SLOTS: String = "topology.bolts.window.slot"

  /* window sizes */
  val hourlyCountWindowSize: Long = 60 * 60 * 1000
  val dailyCountWindowSize: Long = 24 * hourlyCountWindowSize
  val weeklyCountWindowSize: Long = 7 * dailyCountWindowSize

  /* window emission frequency */
  val hourlyCountWindowSlide: Long = 60 * 60 * 1000
  val dailyCountWindowSlide: Long = 24 * hourlyCountWindowSlide
  val weeklyCountWindowSlide: Long = 7 * dailyCountWindowSize
}
