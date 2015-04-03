package varys.util

/**
 * Created by franklab on 15-4-3.
 */

import java.net.{DatagramPacket, InetAddress, DatagramSocket}
import akka.actor.{Props, Actor}
import varys.Logging

case object StartServer
case class GetBottleNeck(host: String, bandwidth: Int)
case class UpdateBandwidth(bandwidth: Int)

class DNBD (
  val p: Int)
  extends Actor with Logging{

  val port = p
  var isStart = false
  var bandwidth = 0


  override def receive = {
    //a blocking call here!!!!
    case GetBottleNeck(host, bandwidth) =>
      val bn = send(host, bandwidth)
      sender ! bn

    case UpdateBandwidth(bw) =>
      bandwidth = bw

    case StartServer =>
      if (!isStart) {
        val start = new Thread(new Runnable {
          override def run(): Unit = {
            bind(port)
          }
        })
        start.start()
        isStart = true
      }

    case _ => logError("DNBD receive something wrong !!!")
  }


  def send(host: String, bandwidth: Int): Int = {
    try {
      logInfo("DNBD bandwidth of Client: %d".format(bandwidth))
      val s = new DatagramSocket()
      val addr = InetAddress.getByName(host)
      val data = bandwidth.toString.getBytes
      //println("client:")
      //data.foreach(print)
      val packet = new DatagramPacket(data, data.length, addr, port)
      s.send(packet)

      //wait for the return
      val recvData = new Array[Byte](1024)
      val recvPacket = new DatagramPacket(recvData, recvData.length)
      s.receive(recvPacket)
      val buf = new String(recvPacket.getData)
      val bwPattern = "[0-9]+".r
      var size = bwPattern.findFirstIn(buf).getOrElse(0).toString.toInt

      logInfo("DNBD bottleneck of Network: %d".format(size))
      return size
    } catch {
      case e: Exception => e.printStackTrace()
        return 0
    }

  }

  def bind(port: Int): Boolean = {
    logInfo("DNBD Server is listening at : %d".format(port))
    val serverSock = new DatagramSocket(port)
    val recvBuff = new Array[Byte](1024)
    while (true) {
      for (i <- 0 until recvBuff.length)
        recvBuff(i) = 0
      val recvPacket = new DatagramPacket(recvBuff, recvBuff.length)
      serverSock.receive(recvPacket)
      val buf = new String(recvPacket.getData)
      val bwPattern = "[0-9]+".r
      var size = bwPattern.findFirstIn(buf).getOrElse(0).toString.toInt
      logInfo("DNBD Server receive bandwidth: %d".format(size))

      //TODO: should get the real bandwith of the server
      val localBw = 100;
      if (size > localBw)
        size = localBw

      //send the bottleneck back to the client
      val clientAddr = recvPacket.getAddress;
      val clientPort = recvPacket.getPort;
      val sendPacket = new DatagramPacket(size.toString.getBytes, size.toString.getBytes.length, clientAddr, clientPort)
      serverSock.send(sendPacket)
    }
    true
  }


}
