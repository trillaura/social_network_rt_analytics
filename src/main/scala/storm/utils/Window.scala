package storm.utils

class Window(size: Int) {

  private var timeframes = new Array[Int](size)
  private var currentIndex: Int = 0
  private var count: Int = 0


  def moveForward(): Int = {
    /* Free the timeframe that is going to go out of the window */
    val lastTimeframeIndex: Int = (currentIndex + 1) % size

    var value = timeframes(lastTimeframeIndex)
    timeframes(lastTimeframeIndex) = 0
    count -= value

    /* Move forward the current index */
    currentIndex = (currentIndex + 1) % size
    value
  }

  def moveForward(positions: Int): Int = {
    var cumulativeValue = 0

    for (_ <- 0 until positions) {
      cumulativeValue += moveForward
    }
    cumulativeValue
  }

  def increment(value: Int): Unit = {
    timeframes(currentIndex) = timeframes(currentIndex) + value
    count += value
  }

  def increment(): Unit = {
    increment(1)
  }

  def estimateTotal(): Int = {
    count
  }

  def computeTotal: Int = {
    var total = 0
    for (i <- timeframes.indices) {
      total += timeframes(i)
    }
    total
  }

}
