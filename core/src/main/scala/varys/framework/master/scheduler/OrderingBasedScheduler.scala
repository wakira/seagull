package varys.framework.master.scheduler

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import varys.framework.master.{CoflowInfo, CoflowState, FlowInfo, SlaveInfo}
import varys.Logging

/**
 * Implementation of a generalized coflow scheduler that works using the 
 * following steps:
 *  1. Order coflows by some criteria.
 *  2. Allocate rates to individual flows of each admitted coflow in that order.
 */
abstract class OrderingBasedScheduler extends CoflowScheduler with Logging {

  val NIC_BitPS = System.getProperty("varys.network.nicMbps", "1024").toDouble * 1048576.0

  //var times: Int = 0


  override def schedule(schedulerInput: SchedulerInput): SchedulerOutput = {
    val markedForRejection = new ArrayBuffer[CoflowInfo]()

    // STEP 1: Sort READY or RUNNING coflows by arrival time
    var sortedCoflows = getOrderedCoflows(schedulerInput.activeCoflows)

    // STEP 2: Perform WSS + Backfilling
    //val sBpsFree = new HashMap[String, Double]().withDefaultValue(NIC_BitPS)
    //val rBpsFree = new HashMap[String, Double]().withDefaultValue(NIC_BitPS)
    val sBpsFree = schedulerInput.sBpsFree
    val rBpsFree = schedulerInput.dBpsFree

    val sUsed = new HashMap[String, Double]()
    val rUsed = new HashMap[String, Double]()
    sBpsFree.foreach {
      keyVal =>
        sUsed.put(keyVal._1, 0.0)
        rUsed.put(keyVal._1, 0.0)
    }

    //test NIC_BitPs
    //times = times + 1
    //println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!%d:\t%f".format(times, NIC_BitPS))
    //println(System.getProperty("varys.network.nicMbps", "1024").getClass)

    for (cf <- sortedCoflows) {
      logInfo("Scheduling " + cf)

      if (markForRejection(cf, sBpsFree, rBpsFree)) {
        markedForRejection += cf
      } else {


        for (flowInfo <- cf.getFlows) {
          val src = flowInfo.source
          val dst = flowInfo.destClient.host

          //DNBD: use bottleneck of every flow instead
          var minFree = math.min(sBpsFree(src), rBpsFree(dst))
          //minFree = math.min(flowInfo.bottleneck, minFree)
          logInfo("Flow %s --> %s bottlneck: %f".format(flowInfo.source, flowInfo.destClient.host, flowInfo.bottleneck))
          if (minFree > 0.0) {
            flowInfo.currentBps = calcFlowRate(flowInfo, cf, minFree)
            if (math.abs(flowInfo.currentBps) < 1e-6) {
              flowInfo.currentBps = 0.0
            }
            flowInfo.lastScheduled = System.currentTimeMillis

            // Remember how much capacity was allocated
            sUsed(src) = sUsed(src) + flowInfo.currentBps
            rUsed(dst) = rUsed(dst) + flowInfo.currentBps
            sBpsFree(src) = sBpsFree(src) - flowInfo.currentBps
            rBpsFree(dst) = rBpsFree(dst) - flowInfo.currentBps

            // Set the coflow as running
            cf.changeState(CoflowState.RUNNING)
          } else {
            flowInfo.currentBps = 0.0
          }
        }

        // Remove capacity from ALL sources and destination for this coflow
        // frankfzw: It's not right here
        /*
        for (sl <- schedulerInput.activeSlaves) {
          val host = sl.host
          sBpsFree(host) = sBpsFree(host) - sUsed(host)
          rBpsFree(host) = rBpsFree(host) - rUsed(host)
        }
        */

        // frankfzw: change the remaining bandwidth here
        /*
        for (sl <- schedulerInput.activeSlaves) {
          val host = sl.host
          if (sUsed.contains(host))
            sBpsFree(host) = sBpsFree(host) - sUsed(host)
          if (rUsed.contains(host))
            rBpsFree(host) = rBpsFree(host) - rUsed(host)

        }
        */

      }
    }

    // STEP2A: Work conservation
    sortedCoflows = sortedCoflows.filter(_.curState == CoflowState.RUNNING)
    for (cf <- sortedCoflows) {
      var totalBps = 0.0
      for (flowInfo <- cf.getFlows) {
        val src = flowInfo.source
        val dst = flowInfo.destClient.host

        var minFree = math.min(sBpsFree(src), rBpsFree(dst))
        //minFree = math.min(flowInfo.bottleneck, minFree)
        if (minFree > 0.0) {
          flowInfo.currentBps += minFree
          sBpsFree(src) = sBpsFree(src) - minFree
          rBpsFree(dst) = rBpsFree(dst) - minFree

          sUsed(src) = sUsed(src) + minFree
          rUsed(dst) = rUsed(dst) + minFree
        }
        
        totalBps += flowInfo.currentBps
      }
      // Update current allocation of the coflow
      cf.setCurrentAllocation(totalBps)
    }

    SchedulerOutput(sortedCoflows, markedForRejection, sUsed, rUsed )
  }

  /**
   *  Returns an ordered list of coflows based on the scheduling policy
   */
  def getOrderedCoflows(
      activeCoflows: ArrayBuffer[CoflowInfo]): ArrayBuffer[CoflowInfo]

  /**
   * Mark a coflow as non-admissible based on some criteria.
   * Overriden for schedulers with admission control (e.g., DeadlineScheduler)
   */
  def markForRejection(
      cf: CoflowInfo, 
      sBpsFree: Map[String, Double], 
      rBpsFree: Map[String, Double]): Boolean =  false

  /**
   * Calculate rate of an individual flow based on the scheduling policy
   */
  def calcFlowRate(
      flowInfo: FlowInfo,
      cf: CoflowInfo,
      minFree: Double): Double

  /** Retuns current time */
  def now() = System.currentTimeMillis
}
