package varys.examples.realsim

import log2coflow.{FlowDescription, CoflowDescription}

import scala.collection.mutable

/**
 * Created by wakira on 15-8-6.
 */
class TraceFileReader(path: String) {
  val source = scala.io.Source.fromFile(path)
  var numNodes = -1

  def coflowsWithStartingTime() = {
    var nextStartTime = 0
    coflows().map(cf => {
      val startTime = nextStartTime
      nextStartTime = startTime + cf.size / (1024*1024*1024)
      (cf, startTime)
    }).toList
  }

  def coflows() = {
    var coflows = new mutable.MutableList[CoflowDescription]

    val twoIntR = """(\d+) (\d+)""".r
    val threeIntR = """(\d+) (\d+) (\d+)""".r

    val reader = source.bufferedReader()
    reader.readLine() match { case twoIntR(numNodes, numCoflows) => {
        this.numNodes = numNodes.toInt
        Range(0, numNodes.toInt).foreach(_ =>
          reader.readLine() match { case twoIntR(cfId, numFlows) =>
              val flows = new mutable.MutableList[FlowDescription]
              Range(0, numFlows.toInt).foreach(fId => {
                reader.readLine() match { case threeIntR(size, s, d) =>
                    flows += new FlowDescription(s, d, size.toInt*1024*1024, fId)
                }})
              coflows += new CoflowDescription(flows.toList)
            }
        )
    }
    coflows
  }

}
