package storm.Bolt

object Config {

  val TOPOLOGY_BOLTS_WINDOW_SIZE_MS: String = "topology.bolts.window.size.ms"
  val TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS: String = "topology.bolts.sliding.interval.duration.ms"
  val TOPOLOGY_BOLTS_WINDOW_SLOTS: String = "topology.bolts.window.slot"

  val hourlyCountWindowSize: Long = 60 * 60 * 1000
  val dailyCountWindowSize: Long = 24 * dailyCountWindowSize
  val weeklyCountWindowSize: Long = 7 * weeklyCountWindowSize

}
