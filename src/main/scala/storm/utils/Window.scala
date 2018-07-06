package storm.utils

class Window(size: Int) {

  val timeFrames = new Array[Int](size)
  private var headSlot = 0
  private var tailSlot = slotAfter(headSlot)

  private var count: Int = 0


  def moveForward(): Int = {
    val value = timeFrames(tailSlot)
    timeFrames(tailSlot) = 0
    advanceHead()
    count -= value
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
    timeFrames(headSlot) = timeFrames(headSlot) + value
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
    for (i <- timeFrames.indices) {
      total += timeFrames(i)
    }
    total
  }

  private def slotAfter(slot: Int) = (slot + 1) % size

  private def advanceHead(): Unit = {
    headSlot = tailSlot
    tailSlot = slotAfter(tailSlot)
  }

}
