package varys.examples.realsim

/**
 * Defines the Akka message protocol between Master and Worker
 * Created by wakira on 15-8-5.
 */

case class PutDescription(id:String, size: Int)
case class GetDescription(id:String)

object ActorProtocol {
  // To be sent by Master
  case class JobMission(cfId: String, putList: List[PutDescription], getList: List[GetDescription])
  case class StartGetting(cfId: String)
  case class JobComplete(cfId: String)
  case object ShutdownWorker

  // To be sent by Worker
  case object WorkerOnline
  case class PutComplete(cfId: String)
  case class GetComplete(cfId: String)
}
