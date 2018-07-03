package storm.Bolt

import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.windowing.TupleWindow

/*
  Warning: When you define topology you must specify withWindow and withTimestampField (or withTimestampExtractor)
 */
class WindowCountBolt extends BaseWindowedBolt {

  import org.apache.storm.task.TopologyContext

  private var collector = null

  def prepare(stormConf: Nothing, context: TopologyContext, collector: Nothing): Unit = {
    this.collector = collector
  }

  override def execute(inputWindow: TupleWindow): Unit = {

  }

}
