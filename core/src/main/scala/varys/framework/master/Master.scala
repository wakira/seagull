package varys.framework.master

import akka.actor._
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}
import akka.util.duration._
import akka.pattern.ask
import akka.dispatch._
import akka.routing._

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._

import varys.framework._
import varys.framework.master.scheduler._
import varys.framework.master.ui.MasterWebUI
import varys.{Logging, Utils, VarysException}
import varys.util.{AkkaUtils, SlaveToBpsMap}

import scala.concurrent.Await

private[varys] class Master(
    systemName:String, 
    actorName: String, 
    host: String, 
    port: Int, 
    webUiPort: Int) 
  extends Logging {
  
  val NUM_MASTER_INSTANCES = System.getProperty("varys.master.numInstances", "1").toInt
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For coflow IDs
  val SLAVE_TIMEOUT = System.getProperty("varys.slave.timeout", "60").toLong * 1000
  
  val CONSIDER_DEADLINE = System.getProperty("varys.master.considerDeadline", "false").toBoolean

  val idToSlave = new ConcurrentHashMap[String, SlaveInfo]()
  val actorToSlave = new ConcurrentHashMap[ActorRef, SlaveInfo]
  val addressToSlave = new ConcurrentHashMap[Address, SlaveInfo]
  val hostToSlave = new ConcurrentHashMap[String, SlaveInfo]

  val idToRxBps = new SlaveToBpsMap
  val idToTxBps = new SlaveToBpsMap

  var nextCoflowNumber = new AtomicInteger()
  val idToCoflow = new ConcurrentHashMap[String, CoflowInfo]()
  val completedCoflows = new ArrayBuffer[CoflowInfo]

  var nextClientNumber = new AtomicInteger()
  val idToClient = new ConcurrentHashMap[String, ClientInfo]()
  val actorToClient = new ConcurrentHashMap[ActorRef, ClientInfo]
  val addressToClient = new ConcurrentHashMap[Address, ClientInfo]

  val webUiStarted = new AtomicBoolean(false)

  //DNBD cache
  val slavesTX = new ConcurrentHashMap[String, Double]()
  val slavesRX = new ConcurrentHashMap[String, Double]()
  val fabric = new ConcurrentHashMap[String, ConcurrentHashMap[String, Double]]()
  val slaveIdToClient = new ConcurrentHashMap[String, ArrayBuffer[String]]()
  val readyToSchedule = new ConcurrentHashMap[String, Int]
  //DNBD thread
  var DNBDT: Thread = null
  val NIC_BitPS = 1024 * 1048576.0 * 0.5

  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())

  private def now() = System.currentTimeMillis
  
  // Create the scheduler object
  val schedulerClass = System.getProperty(
    "varys.master.scheduler", 
    "varys.framework.master.scheduler.SEBFScheduler")

  val coflowScheduler = Class.forName(schedulerClass).newInstance.asInstanceOf[CoflowScheduler]

  def start(): (ActorSystem, Int) = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(
      Props(new MasterActor(host, boundPort, webUiPort)).withRouter(
        RoundRobinRouter(nrOfInstances = NUM_MASTER_INSTANCES)), 
      name = actorName)
    (actorSystem, boundPort)
  }
  
  private[varys] class MasterActor(
      ip: String, 
      port: Int, 
      webUiPort: Int) 
    extends Actor with Logging {

    val webUi = new MasterWebUI(self, webUiPort)

    val masterPublicAddress = {
      val envVar = System.getenv("VARYS_PUBLIC_DNS")
      if (envVar != null) envVar else ip
    }

    override def preStart() {
      logInfo("Starting Varys master at varys://" + ip + ":" + port)
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
      if (!webUiStarted.getAndSet(true)) {
        webUi.start()
      }
      // context.system.scheduler.schedule(0 millis, SLAVE_TIMEOUT millis, self, CheckForSlaveTimeOut)

      //frankfzw: start master of dnbd
      /*
      DNBDT = new Thread (new Runnable {
        override def run(): Unit = {
          logInfo("Master of DNBD is working")
          while (true) {
            updateBpsFree(0.5)
            updateFabric(0.5)
            Thread.sleep(10000)
          }
        }
      })
      DNBDT.run()
      */

    }

    override def postStop() {
      webUi.stop()

      //stop DNBDT
      if (DNBDT != null) {
        DNBDT.interrupt()
      }
    }

    override def receive = {
      case RegisterSlave(id, host, slavePort, slave_webUiPort, slave_commPort, publicAddress) => {
        val currentSender = sender
        logInfo("Registering slave %s:%d".format(host, slavePort))
        if (idToSlave.containsKey(id)) {
          currentSender ! RegisterSlaveFailed("Duplicate slave ID")
        } else {
          addSlave(
            id, 
            host, 
            slavePort, 
            slave_webUiPort, 
            slave_commPort, 
            publicAddress, 
            currentSender)

          // Wait for webUi to bind. Needed when NUM_MASTER_INSTANCES > 1.
          while (webUi.boundPort == None) {
            Thread.sleep(100)
          }

          // frankfzw add slave to readyToSchedule table
          readyToSchedule.put(id, 0)
          
          // context.watch doesn't work with remote actors but helps for testing
          // context.watch(currentSender)  
          currentSender ! RegisteredSlave("http://" + masterPublicAddress + ":" + webUi.boundPort.get)
        }
      }

      case RegisterClient(clientName, host, commPort) => {
        val currentSender = sender
        val st = now
        logTrace("Registering client %s@%s:%d".format(clientName, host, commPort))
        
        if (hostToSlave.containsKey(host)) {
          val client = addClient(clientName, host, commPort, currentSender)

          // context.watch doesn't work with remote actors but helps for testing
          // context.watch(currentSender)
          val slave = hostToSlave(host)

          currentSender ! RegisteredClient(
            client.id, 
            slave.id, 
            "varys://" + slave.host + ":" + slave.port)
          
          logInfo("Registered client " + clientName + " with ID " + client.id + " in " + slave.id +
            "within" + (now - st) + " milliseconds")

          // frankfzw init the tx and rx with the default value
          slavesRX.put(client.host, (NIC_BitPS - idToRxBps.getBps(slave.id) * 8))
          slavesTX.put(client.host, (NIC_BitPS - idToRxBps.getBps(slave.id) * 8))

          //TODO frankfzw, update later
          val temp = new ConcurrentHashMap[String, Double]()
          idToClient.foreach {
            secondKV =>
              temp.put(secondKV._1, NIC_BitPS)
          }
          fabric.put(client.id, temp)
        } else {
          currentSender ! RegisterClientFailed("No Varys slave at " + host)
        }
      }

      case RegisterCoflow(clientId, description) => {
        val currentSender = sender
        val st = now
        logTrace("Registering coflow " + description.name)

        if (CONSIDER_DEADLINE && description.deadlineMillis == 0) {
          currentSender ! RegisterCoflowFailed("Must specify a valid deadline")
        } else {
          val client = idToClient.get(clientId)
          if (client == null) {
            currentSender ! RegisterCoflowFailed("Invalid clientId " + clientId)
          } else {
            val coflow = addCoflow(client, description, currentSender)

            // context.watch doesn't work with remote actors but helps for testing
            // context.watch(currentSender)
            currentSender ! RegisteredCoflow(coflow.id)
            logInfo("Registered coflow " + description.name + " with ID " + coflow.id + " in " + 
              (now - st) + " milliseconds")
          }
        }
      }

      case UnregisterCoflow(coflowId) => {
        removeCoflow(idToCoflow.get(coflowId))
        sender ! true
      }

      case Heartbeat(slaveId, newRxBps, newTxBps) => {
        val slaveInfo = idToSlave.get(slaveId)
        if (slaveInfo != null) {
          slaveInfo.updateNetworkStats(newRxBps, newTxBps)
          slaveInfo.lastHeartbeat = System.currentTimeMillis()

          idToRxBps.updateNetworkStats(slaveId, newRxBps)
          idToTxBps.updateNetworkStats(slaveId, newTxBps)

          //frankfzw upadte slaveTx and slaveRx
          if ((slaveIdToClient.containsKey(slaveId)) && (slaveIdToClient.get(slaveId).size != 0)) {
            for (clientId <- slaveIdToClient.get(slaveId)) {
              val tempClient = idToClient.get(clientId)
              slavesRX.put(tempClient.host, (NIC_BitPS - idToRxBps.getBps(slaveId) * 8))
              slavesTX.put(tempClient.host, (NIC_BitPS - idToRxBps.getBps(slaveId) * 8))
            }
          }
          readyToSchedule.put(slaveId, 1)
          logInfo("Receive heartbeat from %s, RxBps: %f, TxBps: %f".format(slaveId, (NIC_BitPS / 8 - newRxBps), (NIC_BitPS / 8 - newTxBps)))

          // frankfzw check if it's ready to schedule
          var slaveNum: Int = 0
          var times: Int = 0
          for (kv <- readyToSchedule) {
            times = kv._2 + times
            slaveNum = slaveNum + 1
          }
          if (times == slaveNum) {
            for (kv <- readyToSchedule) {
              readyToSchedule.update(kv._1, 0)
            }
            logInfo("Schedule interval, update rate table now")
            schedule()
          }
        } else {
          logWarning("Got heartbeat from unregistered slave " + slaveId)
        }
      }

      case Terminated(actor) => {
        // The disconnected actor could've been a slave or a client; remove accordingly. 
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies
        if (actorToSlave.containsKey(actor))
          removeSlave(actorToSlave.get(actor))
        if (actorToClient.containsKey(actor))  
          removeClient(actorToClient.get(actor))
      }

      case RemoteClientDisconnected(transport, address) => {
        // The disconnected actor could've been a slave or a client; remove accordingly. 
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies
        if (addressToSlave.containsKey(address))
          removeSlave(addressToSlave.get(address))
        if (addressToClient.containsKey(address))  
          removeClient(addressToClient.get(address))
      }

      case RemoteClientShutdown(transport, address) => {
        // The disconnected actor could've been a slave or a client; remove accordingly. 
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies
        if (addressToSlave.containsKey(address))
          removeSlave(addressToSlave.get(address))
        if (addressToClient.containsKey(address))  
          removeClient(addressToClient.get(address))
      }

      case RequestMasterState => {
        sender ! MasterState(
          ip, 
          port, 
          idToSlave.values.toSeq.toArray, 
          idToCoflow.values.toSeq.toArray, 
          completedCoflows.toArray, 
          idToClient.values.toSeq.toArray)
      }

      case CheckForSlaveTimeOut => {
        timeOutDeadSlaves()
        timeOutDeadSlaves()
      }

      case RequestWebUIPort => {
        sender ! WebUIPortResponse(webUi.boundPort.getOrElse(-1))
      }

      case RequestBestRxMachines(howMany, bytes) => {
        sender ! BestRxMachines(idToRxBps.getTopN(
          howMany, bytes).toArray.map(x => idToSlave.get(x).host))
      }

      case RequestBestTxMachines(howMany, bytes) => {
        sender ! BestTxMachines(idToTxBps.getTopN(
          howMany, bytes).toArray.map(x => idToSlave.get(x).host))
      }

      case AddFlows(flowDescs, coflowId, dataType) => {
        val currentSender = sender

        // coflowId will always be valid
        val coflow = idToCoflow.get(coflowId)
        assert(coflow != null)

        val st = now
        flowDescs.foreach { coflow.addFlow }
        logDebug("Added " + flowDescs.size + " flows to " + coflow + " in " + (now - st) + 
          " milliseconds")
        
        currentSender ! true
      }

      case AddFlow(flowDesc) => {
        val currentSender = sender

        // coflowId will always be valid
        val coflow = idToCoflow.get(flowDesc.coflowId)
        assert(coflow != null)

        val st = now
        coflow.addFlow(flowDesc)
        logDebug("Added flow to " + coflow + " in " + (now - st) + " milliseconds")
        
        currentSender ! true
      }

      case GetFlow(flowId, coflowId, clientId, slaveId, _) => {
        logTrace("Received GetFlow(" + flowId + ", " + coflowId + ", " + slaveId + ", " + sender + 
          ")")
        
        val currentSender = sender
        Future { handleGetFlow(flowId, coflowId, clientId, slaveId, currentSender) }
      }

      case GetFlows(flowIds, coflowId, clientId, slaveId, _) => {
        logTrace("Received GetFlows(" + flowIds + ", " + coflowId + ", " + slaveId + ", " + sender + 
          ")")
        
        val currentSender = sender
        Future { handleGetFlows(flowIds, coflowId, clientId, slaveId, currentSender) }
      }

      case FlowProgress(flowId, coflowId, bytesSinceLastUpdate, isCompleted) => {
        // coflowId will always be valid
        val coflow = idToCoflow.get(coflowId)
        assert(coflow != null)

        val st = now
        coflow.updateFlow(flowId, bytesSinceLastUpdate, isCompleted)
        
        logTrace("Received FlowProgress for flow " + flowId + " of " + coflow + " in " + 
          (now - st) + " milliseconds")
      }

      case DeleteFlow(flowId, coflowId) => {
        // TODO: Actually do something; e.g., remove destination?
        // self ! ScheduleRequest
        // sender ! true
      }

      case ScheduleRequest => {

        schedule()

      }
    }

    def handleGetFlows(
        flowIds: Array[String], 
        coflowId: String, 
        clientId: String, 
        slaveId: String, 
        actor: ActorRef) {
      
      logTrace("handleGetFlows(" + flowIds + ", " + coflowId + ", " + slaveId + ", " + actor + ")")

      val client = idToClient.get(clientId)
      assert(client != null)

      val coflow = idToCoflow.get(coflowId)
      assert(coflow != null)
      // assert(coflow.contains(flowId))

      var canSchedule = false
      coflow.getFlowInfos(flowIds) match {
        case Some(flowInfos) => {
          val st = now
          canSchedule = coflow.addDestinations(flowIds, client)

          // TODO: Always returning the default source. 
          // Consider selecting based on traffic etc.
          actor ! Some(GotFlowDescs(flowInfos.map(_.desc)))

          logInfo("Added " + flowIds.size + " destinations to " + coflow + ". " + 
            coflow.numFlowsToRegister + " flows remain to register; in " + (now - st) + 
            " milliseconds")
        }
        case None => {
          // logWarning("Couldn't find flow " + flowId + " of coflow " + coflowId)
          actor ! None
        }
      }

      if (canSchedule) {
        logInfo("Coflow " + coflowId + " ready to be scheduled")
        self ! ScheduleRequest
      }
    }

    def handleGetFlow(
        flowId: String, 
        coflowId: String, 
        clientId: String, 
        slaveId: String, 
        actor: ActorRef) {
      
      logTrace("handleGetFlow(" + flowId + ", " + coflowId + ", " + slaveId + ", " + actor + ")")
      //println("handleGetFlow(" + flowId + ", " + coflowId + ", " + slaveId + ", " + actor + ")")

      val client = idToClient.get(clientId)
      assert(client != null)

      val coflow = idToCoflow.get(coflowId)
      assert(coflow != null)
      // assert(coflow.contains(flowId))

      var canSchedule = false
      coflow.getFlowInfo(flowId) match {
        case Some(flowInfo) => {
          val st = now
          canSchedule = coflow.addDestination(flowId, client)

          // TODO: Always returning the default source. 
          // Considering selecting based on traffic etc.
          actor ! Some(GotFlowDesc(flowInfo.desc))

          logInfo("Added destination to " + coflow + ". " + coflow.numFlowsToRegister + 
            " flows remain to register; in " + (now - st) + " milliseconds")
        }
        case None => {
          // logWarning("Couldn't find flow " + flowId + " of coflow " + coflowId)
          actor ! None
        }
      }

      if (canSchedule) {
        logInfo("Coflow " + coflowId + " ready to be scheduled")
        self ! ScheduleRequest
      }
    }

    def addSlave(
        id: String, 
        host: String, 
        port: Int, 
        webUiPort: Int, 
        commPort: Int,
        publicAddress: String, 
        actor: ActorRef): SlaveInfo = {
      
      // There may be one or more refs to dead slaves on this same node with 
      // different IDs; remove them.
      idToSlave.values.filter(
        w => (w.host == host) && (w.state == SlaveState.DEAD)).foreach(idToSlave.values.remove(_))
      
      val slave = new SlaveInfo(id, host, port, actor, webUiPort, commPort, publicAddress)
      idToSlave.put(slave.id, slave)
      actorToSlave(actor) = slave
      addressToSlave(actor.path.address) = slave
      hostToSlave(slave.host) = slave

      //frankfzw update slave id to client table
      val temp = new ArrayBuffer[String]()
      slaveIdToClient.put(id, temp)
      slave
    }

    def removeSlave(slave: SlaveInfo) {
      slave.setState(SlaveState.DEAD)
      logError("Removing " + slave)
      // Do not remove from idToSlave so that we remember DEAD slaves
      actorToSlave -= slave.actor
      addressToSlave -= slave.actor.path.address
      hostToSlave -= slave.host

      //frankfzw remove dead slave from slaveIdToClient
      slaveIdToClient.remove(slave.id)
    }

    def addClient(clientName: String, host: String, commPort: Int, actor: ActorRef): ClientInfo = {
      val date = new Date(now)
      val client = new ClientInfo(now, newClientId(date), host, commPort, date, actor)
      idToClient.put(client.id, client)
      actorToClient(actor) = client
      addressToClient(actor.path.address) = client

      // frankfzw add client to slaveIdToClient
      val slave = hostToSlave.get(client.host)
      if (slaveIdToClient.containsKey(slave.id)) {
        slaveIdToClient.get(slave.id).append(client.id)
      } else {
        logError("The client comes from nowhere")
      }

      client
    }

    def removeClient(client: ClientInfo) {
      if (client != null && idToClient.containsValue(client)) {
        logTrace("Removing " + client)
        idToClient.remove(client.id)
        actorToClient -= client.actor
        addressToClient -= client.actor.path.address
        client.markFinished()

        // Remove child coflows as well
        client.coflows.foreach(removeCoflow)

        // frankfzw remove client id from slaveIdToClient
        logInfo("Removing client" + client.id + " from " + client.host)
        if (hostToSlave.get(client.host) == null) {
          logError("The client %s comes from nowhere".format(client.id))
        }
        val slave = hostToSlave.get(client.host)
        if (slaveIdToClient.containsKey(slave.id)) {
          slaveIdToClient.get(slave.id) -= client.id
        } else {
          logError("The client %s comes from nowhere".format(client.id))
        }
      }
    }

    def addCoflow(client: ClientInfo, desc: CoflowDescription, actor: ActorRef): CoflowInfo = {
      val now = System.currentTimeMillis()
      val date = new Date(now)
      val coflow = new CoflowInfo(now, newCoflowId(date), desc, client, date, actor)

      idToCoflow.put(coflow.id, coflow)

      // Update its parent client
      client.addCoflow(coflow)  

      coflow
    }

    // TODO: Let all involved clients know so that they can free up local resources
    def removeCoflow(coflow: CoflowInfo) {
      removeCoflow(coflow, CoflowState.FINISHED, true)
    }

    def removeCoflow(coflow: CoflowInfo, endState: CoflowState.Value, reschedule: Boolean) {
      if (coflow != null && idToCoflow.containsValue(coflow)) {
        idToCoflow.remove(coflow.id)
        completedCoflows += coflow  // Remember it in our history
        coflow.markFinished(endState)
        logInfo("Removing " + coflow)

        if (reschedule) {
          self ! ScheduleRequest
        }
      }
    }

    /**
     * Schedule ongoing coflows and flows. 
     * Returns a Boolean indicating whether it ran or not
     */
    def schedule(): Boolean = synchronized {
      var st = now

      // Schedule coflows
      val activeCoflows = idToCoflow.values.toBuffer.asInstanceOf[ArrayBuffer[CoflowInfo]].filter(
        x => x.remainingSizeInBytes > 0 && 
        (x.curState == CoflowState.READY || x.curState == CoflowState.RUNNING))

      //DNBD get the real bottleneck here of the network
      //val realActiveCoflows = calFlowBottleneck(activeCoflows)
      //TODO get the real bandwidth of all source and destination
      val sBpsFree = calSourceBpsFree(activeCoflows)
      val dBpsFree = calDestinationBpsFree(activeCoflows)

      val activeSlaves = idToSlave.values.toBuffer.asInstanceOf[ArrayBuffer[SlaveInfo]]
      //val schedulerOutput = coflowScheduler.schedule(SchedulerInput(realActiveCoflows, activeSlaves, sBpsFree, dBpsFree))
      val schedulerOutput = coflowScheduler.schedule(SchedulerInput(activeCoflows, activeSlaves, sBpsFree, dBpsFree))

      val step12Dur = now - st
      st = now

      // Communicate the schedule to clients
      val activeFlows = schedulerOutput.scheduledCoflows.flatMap(_.getFlows)
      logInfo("START_NEW_SCHEDULE: " + activeFlows.size + " flows in " + 
        schedulerOutput.scheduledCoflows.size + " coflows")
      
      for (cf <- schedulerOutput.scheduledCoflows) {
        val (timeStamp, totalBps) = cf.currentAllocation
        logInfo(cf + " ==> " + (totalBps / 1048576.0) + " Mbps @ " + timeStamp)
      }
      
      activeFlows.groupBy(_.destClient).foreach { tuple => 
        val client = tuple._1
        val flows = tuple._2
        val rateMap = flows.map(t => (t.desc.dataId, t.currentBps)).toMap
        client.actor ! UpdatedRates(rateMap)
      }
      val step3Dur = now - st
      logInfo("END_NEW_SCHEDULE in " + (step12Dur + step12Dur + step3Dur) + " = (" + step12Dur + 
        "+" + step12Dur + "+" + step3Dur + ") milliseconds")


      // Remove rejected coflows
      for (cf <- schedulerOutput.markedForRejection) {
        val rejectMessage = "Cannot meet the specified deadline of " + cf.desc.deadlineMillis + 
          " milliseconds"
        
        cf.parentClient.actor ! RejectedCoflow(cf.id, rejectMessage)
        cf.getFlows.groupBy(_.destClient).foreach { tuple => 
          val client = tuple._1
          client.actor ! RejectedCoflow(cf.id, rejectMessage)
        }

        removeCoflow(cf, CoflowState.REJECTED, false)
      }

      true
    }


    //DNBD update bottleneck of every flow in active coflow before scheduling
    def calFlowBottleneck(activeCoflow: ArrayBuffer[CoflowInfo]): ArrayBuffer[CoflowInfo] = {
      val tempCoflow = activeCoflow
      //val timeout = 500.millis

      //val bottlneckMap = new HashMap[String, String]()
      for (cf <- tempCoflow) {
          cf.getFlows.groupBy(_.destClient).foreach { tuple =>
            //val client = tuple._1

            //TODO find a more elegant way
            for (f <- tuple._2) {
              if ((!idToClient.containsKey(f.getSourceClientId())) || (!idToClient.containsKey(f.destClient.id))) {
                logError("Master calculate flow bottleneck error!! Client doesn't exists!!!")
                logError("\tidToClient Keys: " + idToClient.keys().foreach(println))
                logError("\tidToClient Values: " + idToClient.values())
              } else {
                try {
                  //val sourceClient = idToClient.get(f.getSourceClientId())
                  //val future = sourceClient.actor.ask(GetBottleNeck(tuple._1.host))(timeout)
                  var bdRes = fabric.get(f.getSourceClientId()).get(tuple._1.id)
                  bdRes += f.currentBps
                  fabric(f.getSourceClientId()).put(tuple._1.id, bdRes)
                  slavesTX(f.source) += f.currentBps
                  slavesRX(f.destClient.host) += f.currentBps
                  cf.updateFlowBottleneck(f.getFlowId(), bdRes)
                } catch {
                  case e: Exception =>
                    logError("DNBD: Coflow" + cf.id + " getting bottleneck failed. Details: Source Client ID " + f.getSourceClientId() + " Flow ID " + f.getFlowId())
                }

              }

            }

          }
      }
      tempCoflow
    }

    //
    def calSourceBpsFree(activeCoflow: ArrayBuffer[CoflowInfo]): HashMap[String, Double] = {
      val sBpsFree = new HashMap[String, Double]()
      //val timeout = 500.millis
      slavesTX.foreach{
        keyVal =>
          sBpsFree(keyVal._1) = keyVal._2
      }

      logInfo("DNBD: Master gets all remaining bandwidth of source: " + sBpsFree.values)
      sBpsFree
    }

    def calDestinationBpsFree(activeCoflow: ArrayBuffer[CoflowInfo]): HashMap[String, Double] = {
      val dBpsFree = new HashMap[String, Double]()

      slavesRX.foreach{
        keyVal =>
          dBpsFree(keyVal._1) = keyVal._2
      }

      logInfo("DNBD: Master gets all remaining bandwidth of destinations: " + dBpsFree.values)
      /*
      val timeout = 500.millis

      //TODO may block here, find a better way
      for (cf <- activeCoflow) {
        //get remaining tx bandwidth from sources
        cf.getFlows.groupBy(_.destClient).foreach { tuple =>
          if (!idToClient.containsKey(tuple._1.id)) {
            logError("Master get destination bandwidth error!! Client %s doesn't exists!!!".format(tuple._1.id))
            logError("\tidToClient Keys: " + idToClient.keys().foreach(println))
            logError("\tidToClient Values: " + idToClient.values())
          } else {
            val client = tuple._1
            if (!dBpsFree.contains(client.host)) {
              try {
                val future = client.actor.ask(GetRemainingRX)(timeout)
                val bdRes = akka.dispatch.Await.result(future, timeout).asInstanceOf[Int]
                dBpsFree(client.host) = bdRes.toDouble
                logInfo("DNBD: Remaining rx of " + client.host + " is " + bdRes.toDouble)
              }
            }
          }

        }
      }
      logInfo("DNBD: Master gets all remaining bandwidth of destinations: " + dBpsFree.values)
      */
      dBpsFree
    }

    def updateBpsFree(rate: Double): Unit = {
      val timeout = 500.millis
      idToClient.foreach {
        keyVal =>
          try {
            val future = keyVal._2.actor.ask(GetRemainingTX)(timeout)
            val bdRes = akka.dispatch.Await.result(future, timeout).asInstanceOf[Int]
            slavesTX.put(keyVal._2.host, bdRes.toDouble * rate)
          } catch {
            case e: Exception =>
              logError("DNBD: Calculate Source TX timeout !!!")
          }

          try {
            val future = keyVal._2.actor.ask(GetRemainingRX)(timeout)
            val bdRes = akka.dispatch.Await.result(future, timeout).asInstanceOf[Int]
            slavesRX.put(keyVal._2.host, bdRes.toDouble * rate)
          } catch {
            case e: Exception =>
              logError("DNBD: Calculate Source RX timeout !!!")
          }

      }
    }

    def updateFabric(rate: Double): Unit = {
      val timeout = 500.millis
      idToClient.foreach{
        keyVal =>
          idToClient.foreach{
            secondKV =>
              if (secondKV._1 != keyVal._1) {
                try {
                  val future = keyVal._2.actor.ask(GetBottleNeck(secondKV._2.host))(timeout)
                  val bdRes = akka.dispatch.Await.result(future, timeout).asInstanceOf[Int]
                  fabric.get(keyVal._1).update(secondKV._1, bdRes.toDouble * rate)
                } catch {
                  case e: Exception =>
                    logError("DNBD update time out: from" + keyVal._1 + " to " + secondKV._1)
                }
              }
          }
      }
    }


    /** 
     * Generate a new coflow ID given a coflow's submission date 
     */
    def newCoflowId(submitDate: Date): String = {
      "COFLOW-%06d".format(nextCoflowNumber.getAndIncrement())
    }

    /** 
     * Generate a new client ID given a client's connection date 
     */
    def newClientId(submitDate: Date): String = {
      "CLIENT-%06d".format(nextClientNumber.getAndIncrement())
    }

    /** 
     * Check for, and remove, any timed-out slaves 
     */
    def timeOutDeadSlaves() {
      // Copy the slaves into an array so we don't modify the hashset while iterating through it
      val expirationTime = System.currentTimeMillis() - SLAVE_TIMEOUT
      val toRemove = idToSlave.values.filter(_.lastHeartbeat < expirationTime).toArray
      for (slave <- toRemove) {
        logWarning("Removing slave %s because we got no heartbeat in %d seconds".format(
          slave.id, SLAVE_TIMEOUT))
        removeSlave(slave)
      }
    }
  }
}

private[varys] object Master {
  private val systemName = "varysMaster"
  private val actorName = "Master"
  private val varysUrlRegex = "varys://([^:]+):([0-9]+)".r

  def main(argStrings: Array[String]) {
    val args = new MasterArguments(argStrings)
    val masterObj = new Master(systemName, actorName, args.ip, args.port, args.webUiPort)
    val (actorSystem, _) = masterObj.start()
    actorSystem.awaitTermination()

  }

  /** 
   * Returns an `akka://...` URL for the Master actor given a varysUrl `varys://host:ip`. 
   */
  def toAkkaUrl(varysUrl: String): String = {
    varysUrl match {
      case varysUrlRegex(host, port) =>
        "akka://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new VarysException("Invalid master URL: " + varysUrl)
    }
  }


}
