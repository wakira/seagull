package varys.examples

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework.client._
import varys.framework._

private[varys] object ReceiverClientFake extends Logging {

  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("USAGE: ReceiverClientFake <masterUrl> <coflowId> [dataName]")
      System.exit(1)
    }
    
    val url = args(0)
    val coflowId = args(1)
    val DATA_NAME = if (args.length > 2) args(2) else "DATA"

    val listener = new TestListener
    val client = new VarysClient("ReceiverClientFake", url, listener)
    client.start()
    //DNBD start for test
    //client.startDNBD(5678, "p3p1")
    
    //Thread.sleep(20000)
    
    println("Trying to retrieve " + DATA_NAME)
    val st = System.currentTimeMillis()
    client.getFake(DATA_NAME, coflowId)
    val interval = System.currentTimeMillis() - st
    println("Got " + DATA_NAME + ". Now waiting to die. It takes " + interval + " ms")
    //logInfo("Got " + DATA_NAME + ". Now waiting to die. It takes " + interval + " ms")
    
    client.awaitTermination()
  }
}
