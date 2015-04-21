package varys.util

/**
 * Created by franklab on 15-4-3.
 */

import java.net.{DatagramPacket, InetAddress, DatagramSocket}
import akka.actor.{Props, Actor}
import varys.Logging
import scala.sys.process._

case object StartServer
case class GetBottleNeck(host: String, bandwidth: Int)
case class UpdateBandwidth(bandwidth: Double)

class DNBD (
  val p: Int,
  val eth: String)
  extends Actor with Logging{

  val port = p
  val interface = eth
  var isStart = false
  var bandwidth: Double = 0


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

  override def preStart(): Unit = {
    //TODO get bandwidth of nic "interface"
  }


  def send(host: String, bandwidth: Int): Int = {
    try {
      logInfo("DNBD bandwidth of Client: %d".format(bandwidth))
      val s = new DatagramSocket()
      val addr = InetAddress.getByName(host)

      //get remaining bw of this end host
      val bd = new Bandwidth(interface)
      val transRate = bd.readTx()
      val data = (bandwidth - transRate).toString.getBytes
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
      val size = bwPattern.findFirstIn(buf).getOrElse(0).toString.toInt

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

      val bd = new Bandwidth(interface)
      val recvRate = bd.readRx();
      if (size > recvRate)
        size = recvRate

      //send the bottleneck back to the client
      val clientAddr = recvPacket.getAddress;
      val clientPort = recvPacket.getPort;
      val sendPacket = new DatagramPacket(size.toString.getBytes, size.toString.getBytes.length, clientAddr, clientPort)
      serverSock.send(sendPacket)
    }
    true
  }

  class Bandwidth(val interface: String) {
    val eth = interface

    def readRx(): Int = {
      var rx: Int = 0

      val rx0Str = ("cat /sys/class/net/%s/statistics/rx_bytes".format(interface) !!)
      val rx0 = rx0Str.substring(0, (rx0Str.length - 1)).toInt
      //println(rx0)
      //rx0Str.foreach(println)
      Thread.sleep(100)
      val rx1Str = ("cat /sys/class/net/%s/statistics/rx_bytes".format(interface) !!)
      val rx1 = rx1Str.substring(0, rx1Str.length - 1).toInt
      //println(rx1Str)
      rx = (rx1 - rx0) * 10
      //println(rx)
      return rx
    }

    def readTx(): Int = {
      var tx: Int = 0
      //TODO: development
      val tx0Str = ("cat /sys/class/net/%s/statistics/tx_bytes".format(interface) !!)
      val tx0 = tx0Str.substring(0, (tx0Str.length - 1)).toInt
      Thread.sleep(100)
      val tx1Str = ("cat /sys/class/net/%s/statistics/tx_bytes".format(interface) !!)
      val tx1 = tx1Str.substring(0, tx1Str.length - 1).toInt
      //println(rx1Str)
      tx = (tx1 - tx0) * 10
      //println(tx)
      return tx
    }
  }

}
