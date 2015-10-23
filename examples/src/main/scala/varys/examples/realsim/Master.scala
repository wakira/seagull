package varys.examples.realsim

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import collection.JavaConversions.enumerationAsScalaIterator

import akka.actor._
import akka.routing.RoundRobinRouter
import varys.Logging
import varys.framework.{CoflowType, CoflowDescription}
import varys.framework.client.VarysClient
import varys.util.AkkaUtils

import scala.collection.mutable


/**
 * Created by wakira on 15-8-5.
 */


class Master(varysUrl: String, jobs: List[(log2coflow.CoflowDescription, Int)], numNodes: Int)
  extends Actor with Logging {
  private val sortedJobs = jobs.sortBy(_._2) // sort jobs by their start time (if not already sorted = =)
  private val numOnlineWorkers: AtomicInteger = new AtomicInteger()
  private val nodeToActor = new ConcurrentHashMap[String, ActorRef]()
  private val workerList = new mutable.MutableList[ActorRef]
  private val jobDistributionThread: JobDistributionThread = new JobDistributionThread
  private var varysClient: VarysClient = null

  private val cfIdToCoflowDescription = new ConcurrentHashMap[String, log2coflow.CoflowDescription]
  private val numPutCompleted = new ConcurrentHashMap[String, AtomicInteger]
  private val numGetCompleted = new ConcurrentHashMap[String, AtomicInteger]
  private val cfIdToStartTime = new ConcurrentHashMap[String, Long]
  private val cfIdToEndTime = new ConcurrentHashMap[String, Long]
  private val numCompletedJobs = new AtomicInteger()

  private class JobDistributionThread extends Thread("JobDistribution") with Logging {
    override def run(): Unit = {
      var curTime = 0
      sortedJobs.foreach(job => {
        val cf = job._1
        val st = job._2
        Thread.sleep(st - curTime)
        curTime = st

        val cfId = varysClient.registerCoflow(new CoflowDescription(
          cf.hashCode().toString, CoflowType.SHUFFLE, cf.width, cf.size)) // TODO add deadline
        // initialize data structures to track states of completion
        cfIdToCoflowDescription.put(cfId, cf)
        numPutCompleted.put(cfId, new AtomicInteger())
        numGetCompleted.put(cfId, new AtomicInteger())
        // construct put list and get list
        cf.nodes.foreach(node => {
          val putList = cf.flows.filter(_.source == node).map(flow =>
            new PutDescription("flow-" + flow.uid.toString, flow.size)
          )
          val getList = cf.flows.filter(_.dest == node).map(flow =>
            new GetDescription("flow-" + flow.uid.toString)
          )
          nodeToActor.get(node) ! ActorProtocol.JobMission(cfId, putList, getList)
        })
      }
      )
    }
  }

  private def distributeJob() = {
    jobDistributionThread.start()
  }

  private def stopAllWorkers() = {
    workerList.foreach(worker =>
      worker ! ActorProtocol.ShutdownWorker
    )
  }

  private def printStatistics() = {
    // print each coflow's start time, completion time, and CCT
    val numCoflows = cfIdToCoflowDescription.size()
    val sizeToCCT = new mutable.HashMap[Int, Long]
    var cctSum = 0L
    enumerationAsScalaIterator(cfIdToCoflowDescription.keys()).foreach(cfId => {
      val cf = cfIdToCoflowDescription.get(cfId)
      val st = cfIdToStartTime.get(cfId)
      val et = cfIdToEndTime.get(cfId)
      val cct = et - st
      sizeToCCT.put(cf.size, cct)
      cctSum += cct
      println("%s with size:%d width:%d starts at %d and ends at %d, CCT:%d".format(
        cfId, cf.size, cf.width, st, et, cct))
    })
    // compute and print average CCT
    println("Average CCT is %d".format(cctSum/numCoflows))
    // compute and print 90%(?)-smaller CCT
    val smaller90cct = sizeToCCT.toSeq.sortBy(_._1).slice(0, (numCoflows*0.9).toInt).foldLeft(0L)(_ + _._2)
    println("Average CCT of 90 percent smaller coflows is %d".format(smaller90cct/numCoflows))
    val smaller80cct = sizeToCCT.toSeq.sortBy(_._1).slice(0, (numCoflows*0.8).toInt).foldLeft(0L)(_ + _._2)
    println("Average CCT of 80 percent smaller coflows is %d".format(smaller80cct/numCoflows))
  }

  override protected def receive: Receive = {
    case Master.Init =>
      // put (if any) initialization code after preStart here
    case ActorProtocol.WorkerOnline =>
      val currentWorker = sender
      logInfo(sender.toString() + "is online")
      workerList += currentWorker
      nodeToActor.put(numOnlineWorkers.getAndIncrement().toString, currentWorker)
      if (numOnlineWorkers.get() == numNodes)
        distributeJob()
    case ActorProtocol.PutComplete(cfId) =>
      // increase counter for that job, send StartGetting to every worker after received from all workers
      if (numPutCompleted.get(cfId).incrementAndGet() == cfIdToCoflowDescription.get(cfId).nodes.size) {
        cfIdToStartTime.put(cfId, System.currentTimeMillis)
        cfIdToCoflowDescription.get(cfId).nodes.foreach(node =>
          nodeToActor.get(node) ! ActorProtocol.StartGetting(cfId)
        )
      }
    case ActorProtocol.GetComplete(cfId) =>
      // increase counter for that job, send JobComplete to every worker after received from all workers
      if (numGetCompleted.get(cfId).incrementAndGet() == cfIdToCoflowDescription.get(cfId).nodes.size) {
        cfIdToEndTime.put(cfId, System.currentTimeMillis)
        cfIdToCoflowDescription.get(cfId).nodes.foreach(node =>
          nodeToActor.get(node) ! ActorProtocol.JobComplete(cfId)
        )
        varysClient.unregisterCoflow(cfId)
        if (numCompletedJobs.incrementAndGet() == jobs.length) {
          stopAllWorkers()
          printStatistics()
          context.system.shutdown()
        }
      }
  }

  override def preStart(): Unit = {
    // start VarysClient
    logInfo("Starting RealSimMaster, connecting to varys master...")
    varysClient = new VarysClient("RealSimMaster", varysUrl, new TestListener)
    varysClient.start()
  }
}

object Master {
  // define messages used within this file
  val systemName = "realSimMaster"
  val actorName = "Master"
  val host = varys.Utils.localHostName()
  var port = 4728
  case object Init

  def main(args: Array[String]) {
    // read jobs description file and generate JobDescriptions
    if (args.length != 2) {
      println("Parameters: <varysUrl> <inputFile>")
    }
    val varysUrl = args(0)
    val pathToFile = args(1)
    val tfr = new TraceFileReader(pathToFile)
    val jobs = tfr.coflowsWithStartingTime()
    val numNodes = tfr.numNodes
    assert(numNodes != -1)

    // start Akka system and run master (use AkkaUtils)
    val (system, _) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = system.actorOf(
      Props(new Master(varysUrl, jobs, numNodes)).withRouter(
        RoundRobinRouter(nrOfInstances = 1)), name = actorName)
    actor ! Init
    system.awaitTermination()
  }

  def toAkkaUrl(host: String, port: Int): String =
    "akka://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
}
