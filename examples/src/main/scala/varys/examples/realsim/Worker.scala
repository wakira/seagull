package varys.examples.realsim

import java.util.concurrent.ConcurrentHashMap
import java.io._

import varys.{Utils, Logging}
import varys.framework.client.{ClientListener, VarysClient}
import varys.util.AkkaUtils
import akka.actor.{Props, ActorRef, Actor}
import scala.concurrent.{Future, Await, ExecutionContext}

/**
 * Created by wakira on 15-8-5.
 */

/* Listener used for VarysClient */
class TestListener extends ClientListener with Logging {
  def connected(id: String) {
    logInfo("Connected to varys master, got client ID " + id)
  }

  def disconnected() {
    logInfo("Disconnected from varys master")
    System.exit(1)
  }

  override def coflowRejected(coflowId: String, rejectMessage: String): Unit = {
    logInfo(coflowId + " was rejected: " + rejectMessage)
  }
}

class Worker(val varysUrl: String, masterUrl: String) extends Actor with Logging {
  var varysClient: VarysClient = null
  var master: ActorRef = null
  private val realSimMasterUrlRegex = "([^:]+):([0-9]+)".r
  val cfIdToGetList = new ConcurrentHashMap[String, List[GetDescription]]

  implicit val futureExecContext = ExecutionContext.fromExecutor(varys.Utils.newDaemonCachedThreadPool())

  override def preStart(): Unit = {
    // start VarysClient
    logInfo("Starting RealSimWorker, connecting to varys master...")
    varysClient = new VarysClient("RealSimWorker", varysUrl, new TestListener)
    varysClient.start()
  }

  override protected def receive: Receive = {
    case Worker.Init =>
      // parse masterUrl
      var masterHost: String = null
      var masterPort: Int = 0
      masterUrl match {
        case realSimMasterUrlRegex(h, p) =>
          masterHost = h
          masterPort = p.toInt
        case _ =>
          logError("Invalid traceMasterUrl: " + masterUrl)
          logInfo("traceMasterUrl should be given as host:port")
          System.exit(1)
      }
      // get Master actor and send it ActorProtocol.WorkerOnline
      try {
        master = context.actorFor(Master.toAkkaUrl(masterHost, masterPort))
        master ! ActorProtocol.WorkerOnline
      } catch {
        case e: Exception =>
          logError("Failed to connect to master", e)
          System.exit(1)
      }
    case ActorProtocol.JobMission(cfId, putList, getList) =>
      cfIdToGetList.put(cfId, getList)
      val future = Future.traverse(putList)(x => Future {
        val tempFile = new File("tracedriventmp", "coflow-"+cfId+"-flow-"+x.id.toString+".tmp")
        val putData = Array.tabulate[Byte](x.size)(_.hashCode().toByte)
        val outputStream = new FileOutputStream(tempFile)
        try {
          outputStream.write(putData)
        } finally {
          outputStream.close()
        }
        varysClient.putFile(x.id, tempFile.getAbsolutePath, cfId, x.size, 1)
        logInfo("Varys put id " + x.id + " with size " + x.size.toString)
      })
      future.onComplete(_ => master ! ActorProtocol.PutComplete(cfId))
    case ActorProtocol.StartGetting(cfId) =>
      val future = Future.traverse(cfIdToGetList.get(cfId))(x => Future{
        varysClient.getFile(x.id, cfId)
      })
      future.onComplete(_ => master ! ActorProtocol.GetComplete(cfId))
    case ActorProtocol.JobComplete(cfId) =>
      cfIdToGetList.remove(cfId)
  }
}

object Worker {
  case object Init
  val systemName = "realSimWorker"
  val actorName = "Worker"
  val host = Utils.localHostName()
  val port = 0


  def main(args: Array[String]): Unit = {
    val varysUrl = args(0)
    val masterUrl = args(1)

    // start Akka system and run Worker (use AkkaUtils)
    val (system, _) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = system.actorOf(Props(new Worker(varysUrl, masterUrl)), name = actorName)
    actor ! Init
    system.awaitTermination()
  }
}
