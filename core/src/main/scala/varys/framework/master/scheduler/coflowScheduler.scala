package varys.framework.master.scheduler

import scala.collection.mutable.{HashMap, ArrayBuffer}

import varys.framework.master.{CoflowInfo, SlaveInfo}

/**
 * Primary interface of coflow schedulers. 
 *
 * Callbacks methods from master must be defined here to address various events. 
 *
 * User(s) of this must *not* block inside the callback methods.
 */
trait CoflowScheduler {
  def schedule(schedulerInput: SchedulerInput): SchedulerOutput
}

/**
 * Container class to carry scheduler input
 */
case class SchedulerInput(
    activeCoflows: ArrayBuffer[CoflowInfo],
    activeSlaves: ArrayBuffer[SlaveInfo],
    sBpsFree: HashMap[String, Double],
    dBpsFree: HashMap[String, Double]
)

/**
 * Container class to carry back scheduler output
 */
case class SchedulerOutput(
    scheduledCoflows: ArrayBuffer[CoflowInfo],
    markedForRejection: ArrayBuffer[CoflowInfo],
    sBpsUsed: HashMap[String, Double],
    dBpsUsed: HashMap[String, Double]
)
