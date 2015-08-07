package varys.examples.realsim

/**
 * Defines the Akka message protocol between Master and Worker
 * Created by wakira on 15-8-5.
 */

class PutDescription(val id:String, val size: Int)
class GetDescription(val id:String)

object ActorProtocol {
  // To be sent by Master
  case class JobMission(cfId: String, putList: List[PutDescription], getList: List[GetDescription])
  case class StartGetting(cfId: String)
  case class JobComplete(cfId: String)
  case object ShutdownWorker

  // To be sent by Worker
  case object WorkerOnline
  case class JobMissionAck(cfId: String)
  case class PutComplete(cfId: String)
  case class GetComplete(cfId: String)
}
