package varys.util

/**
 * Created by franklab on 15-4-3.
 */

import java.net.{DatagramPacket, InetAddress, DatagramSocket}
import akka.actor.{Props, Actor}
import varys.Logging
import scala.sys.process._

case object StartServer
case class GetBottleNeck(host: String)
case class UpdateBandwidth(bandwidth: Int)

class DNBD (
  val p: Int,
  val eth: String)
  extends Actor with Logging{

  val port = p
  val interface = eth
  var isStart = false
  var bandwidth: Int = 0


  override def receive = {
    //a blocking call here!!!!
    case GetBottleNeck(host) =>
      val bn = send(host)
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
    val bd = new Bandwidth(interface)
    bandwidth = bd.getBW()
    logInfo("DNBD bandwidth of host is: %d B/S".format(bandwidth))
  }


  def send(host: String): Int = {
    try {
      val s = new DatagramSocket()
      val addr = InetAddress.getByName(host)

      //get remaining bw of this end host
      val bd = new Bandwidth(interface)
      val transRate = bd.readTx()
      val data = (bandwidth - transRate).toString.getBytes
      logInfo("DNBD TX bandwidth of Source: %d".format(bandwidth - transRate))
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
      logInfo("DNBD destination receive bandwidth: %d".format(size))

      val bd = new Bandwidth(interface)
      val recvRate = bd.readRx();
      val rxRate = bandwidth - recvRate;
      logInfo("DNBD RX bandwidth of destination: %d".format(rxRate))
      if (size > rxRate)
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
      val pattern = "[0-9]+".r
      //TODO it's may not safe here
      val rx0 = pattern.findFirstIn(rx0Str).getOrElse(0).toString
      //println(rx0)
      //rx0Str.foreach(println)
      Thread.sleep(100)
      val rx1Str = ("cat /sys/class/net/%s/statistics/rx_bytes".format(interface) !!)
      //TODO it's may not safe here
      val rx1 = pattern.findFirstIn(rx1Str).getOrElse(0).toString
      //println(rx1Str)
      rx = stringMinus(rx1, rx0) * 10
      //println(rx)
      return rx
    }

    def readTx(): Int = {
      var tx: Int = 0
      val tx0Str = ("cat /sys/class/net/%s/statistics/tx_bytes".format(interface) !!)
      val pattern = "[0-9]+".r
      //TODO it's may not safe here
      val tx0 = pattern.findFirstIn(tx0Str).getOrElse(0).toString
      Thread.sleep(100)
      val tx1Str = ("cat /sys/class/net/%s/statistics/tx_bytes".format(interface) !!)
      //TODO it's may not safe here
      val tx1 = pattern.findFirstIn(tx1Str).getOrElse(0).toString
      //println(rx1Str)
      tx = stringMinus(tx1, tx0) * 10
      //println(tx)
      return tx
    }

    def getBW(): Int = {
      //val res = "echo 05806056966" #| "sudo -S ethtool eth0" #| "grep Speed" !
      val buffer = new StringBuffer()
      val cmd = Seq("ethtool", eth)
      val lines = cmd lines_! ProcessLogger(buffer append _)
      //println(lines)
      var bwStr = ""
      for (s <- lines if s.contains("Speed")) bwStr = s
      val bwPattern = "[0-9]+".r
      val bw = bwPattern.findFirstIn(bwStr).getOrElse(0).toString.toInt
      val ret = bw / 8 * 1024 * 1024
      //println(ret)
      ret

    }

    def stringMinus(l: String, r: String): Int = {
      var res = 0
      var size = l.size
      if (r.size < size)
        size = r.size
      for (x <- size -1 to 0 by -1) {
        res = res + (l.charAt(x) - r.charAt(x)) * (scala.math.pow(10, (size - 1 - x)).toInt)
      }
      res
    }
  }

}
