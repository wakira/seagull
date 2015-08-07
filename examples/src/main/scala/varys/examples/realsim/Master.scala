package varys.examples.realsim

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

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
  val sortedJobs = jobs.sortBy(_._2) // sort jobs by their start time (if not already sorted = =)
  val numOnlineWorkers: AtomicInteger = new AtomicInteger()
  val nodeToActor = new ConcurrentHashMap[String, ActorRef]()
  val workerList = new mutable.MutableList[ActorRef]
  val jobDistributionThread: JobDistributionThread = new JobDistributionThread
  var varysClient: VarysClient = null

  val cfIdToCoflowDescription = new ConcurrentHashMap[String, log2coflow.CoflowDescription]
  val numPutCompleted = new ConcurrentHashMap[String, AtomicInteger]
  val numGetCompleted = new ConcurrentHashMap[String, AtomicInteger]
  val numNodesInCoflow = new ConcurrentHashMap[String, Int]
  val cfIdToStartTime = new ConcurrentHashMap[String, Long]
  val cfIdToEndTime = new ConcurrentHashMap[String, Long]

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
        numNodesInCoflow.put(cfId, cf.nodes.size)
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
    jobDistributionThread.run()
  }

  override protected def receive: Receive = {
    case Master.Init =>
      // put (if any) initialization code after preStart here
    case ActorProtocol.WorkerOnline =>
      workerList += sender
      nodeToActor.put(numOnlineWorkers.getAndIncrement().toString, sender)
      if (numOnlineWorkers.get() == numNodes)
        distributeJob()
    case ActorProtocol.PutComplete(cfId) =>
      // increase counter for that job, send StartGetting to every worker after received from all workers
      if (numPutCompleted.get(cfId).incrementAndGet() == numNodesInCoflow.get(cfId)) {
        cfIdToStartTime.put(cfId, System.currentTimeMillis)
        cfIdToCoflowDescription.get(cfId).nodes.foreach(node =>
          nodeToActor.get(node) ! ActorProtocol.StartGetting(cfId)
        )
      }
    case ActorProtocol.GetComplete(cfId) =>
      // increase counter for that job, send JobComplete to every worker after received from all workers
      if (numGetCompleted.get(cfId).incrementAndGet() == numNodesInCoflow.get(cfId)) {
        cfIdToEndTime.put(cfId, System.currentTimeMillis)
        cfIdToCoflowDescription.get(cfId).nodes.foreach(node =>
          nodeToActor.get(node) ! ActorProtocol.JobComplete(cfId)
        )
        varysClient.unregisterCoflow(cfId)
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
