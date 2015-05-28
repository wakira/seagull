package varys.util

/**
 * Created by franklab on 15-5-27.
 */

import java.util

import akka.actor.{ActorRef, Props, ActorSystem, Actor}
import org.jnetpcap.Pcap
import org.jnetpcap.PcapBpfProgram
import org.jnetpcap.PcapIf
import org.jnetpcap.packet.PcapPacket
import org.jnetpcap.packet.PcapPacketHandler
import org.jnetpcap.protocol.tcpip.Tcp
import varys.Logging
import varys.framework._




class ECN (val ifName: String) extends Actor with Logging{

  val _ifName = ifName

  class controlTask(sender: ActorRef) extends Runnable {
    val sender_ = sender
    override def run: Unit = {
      start(sender_)
    }
  }

  override def receive ={
    case StartECN =>
      val task = new Thread(new controlTask(sender))
      task.run()
    case _ =>
      logError("Controller of ECN receive something wrong")
  }

  def start(sender: ActorRef): Unit = {
    val devs = new util.ArrayList[PcapIf]()
    val errbuf = new java.lang.StringBuilder()
    val r = Pcap.findAllDevs(devs, errbuf)
    if (r == Pcap.NOT_OK || devs.isEmpty) {
      println("Cannot read devices, %s".format(errbuf.toString()))

    }
    val targetIf = for {i <- 0 to (devs.size() - 1)
                        if (devs.get(i).getName.compareTo(_ifName) == 0)
    } yield devs.get(i)

    if (targetIf.isEmpty) {
      logError("Cannot find the device named %s".format(_ifName))

    }

    val interface = targetIf(0)
    val snaplen = 64 * 1024;
    val flags = Pcap.MODE_PROMISCUOUS
    val timeout = 10 * 1000
    val pcap = Pcap.openLive(interface.getName, snaplen, flags, timeout, errbuf)

    if (pcap == null) {
      logError("Cannot open devices, %s".format(errbuf.toString()))

    }

    val ips = for {i <- 0 to interface.getAddresses.size() - 1
                   if (interface.getAddresses.get(i).getAddr.toString.contains("INET4"))
    } yield interface.getAddresses.get(i).getAddr.toString

    if (ips.isEmpty) {
      logError("Cannot find the ip address of %s".format(_ifName))
    }

    val ip = ips(0).substring((ips(0).indexOf(':') + 1), (ips(0).size - 1))
    logInfo("IP of %s is %s".format(_ifName, ip))

    val program = new PcapBpfProgram()
    val expression = "tcp and dst ".concat(ip)
    val optimize = 0
    val netmask = 0xffffff00

    if (pcap.compile(program, expression, optimize, netmask) != Pcap.OK) {
      logError("Error: %s".format(pcap.getErr))

    }

    if (pcap.setFilter(program) != Pcap.OK) {
      logError("Error: %s".format(pcap.getErr))

    }

    val jpacketHandler = new PcapPacketHandler[String] {
      val tcp = new Tcp()
      var total: Int = 0xffff
      var numCE: Int = 0x0
      var fraction = 0.0
      def nextPacket(packet: PcapPacket, user: String) {
        logDebug("Received packet at %s with %s".format(packet.getCaptureHeader.timestampInMillis(), user))
        if (packet.hasHeader(tcp)) {
          logDebug("\t Find tcp with prot %d, congestion condition: %b".format(tcp.destination(), tcp.flags_CWR()))
          if (tcp.flags_CWR())
            numCE = (numCE << 1 | 0x1) & 0xffff
          for (i <- 0 to 15)
            fraction = fraction + (numCE << i & 0x1).toDouble
          fraction = fraction / 16
          sender ! UpdateRate(fraction)
        }
      }

    }


    pcap.loop(Pcap.LOOP_INFINITE, jpacketHandler, _ifName)
    pcap.close

  }
}
