package main

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.io.Source

/**
 * Created by Christos on 10/3/2016.
 */
class Query2 {

  //string that holds the last output
  var lastOutput  = ""
  //map that holds id, timestamp and description of comments
  var comments: Map[Long,(Long,String)] = Map()
  //map that holds the graphs with the corresponding graphs and ranges and timestamps
  var graphs: Map[Long,(Graph, Int, Long)] = Map()
  var latSum = 0L
  var latAmt = 0
  var latStart = 0L
  var outArr = Array.empty[String]
  var sortedArray = Array.empty[(Long, Int, String)]

  def run(k: Int, d: Int, clockStart: Long) = {

    val log = new FileWriter("log.txt", true)
    //    println("Q2: Initiated log...")
    //    log.write("Q2: Initiated log...\nLog is responding...\n")

    //============== Query 2 Start ==============
    try {
      run2(k, d, clockStart, log)
    }
    catch {
      case e: Exception => log.write("Q2 (Exception):\t" + e.toString + "\n\n" + e.getStackTraceString)
    }

    log.close

    //============== Query 2 End ==============

  }

  private def run2(k: Int, d: Int, clockStart: Long, log: FileWriter) = {

    log.write("\n\n*" + Calendar.getInstance().getTime + ": Q2:\tStarted.\n")
    //map that holds friendships
    var friendships: Map[Long,Array[Long]] = Map()
    //map that holds user likes history
    var likes: Map[Long, Set[Long]] = Map()

    val start = clockStart/1000

    for(line <- Source.fromFile("merged2.dat")("UTF-8").getLines) {
      if (line != "") {
        latStart = System.currentTimeMillis * 1000000 / 1000
        val inStream = line.split("[|]")
        val event = inStream.length
        val now = getMillis(inStream.head)
        //decay the map to only hold graphs of last d seconds
        graphs = graphs.filter(t => t._2._3 >= now - d * 1000)
        //decay the sorted array of comments
        sortedArray = sortedArray.filter(x => graphs.contains(x._1))
        //if input stream is a comment, just add to map
        if (event == 7 || event == 6) {
          val comment_id = inStream(1).toLong
          val description = inStream(3)
          graphs += comment_id ->(new Graph, 0, now)
          comments += comment_id ->(now, description)
          //get current output
          decayOutput(k, inStream.head)
        } //if input stream is either like or friendship
        else if (event == 3) {
          //if input stream is a like
          //get comment id
          val comment_id = inStream(2).toLong
          if (graphs.contains(comment_id) && comments.contains(comment_id)) {
            //get user id that liked the comment
            val user_id = inStream(1).toLong
            //get graph timestamp
            val timestamp = graphs(comment_id)._3
            //get the comment graph
            val g = graphs(comment_id)._1
            //if a graph has not been liked yet, add graph
            if (g.range == 0) {
              //FIRST LIKE
              //add vertex
              g.addVertex(user_id)
              //update map of graphs
              graphs += comment_id ->(g, 1, timestamp)
              sortedArray ++= Array((comment_id, 1, comments(comment_id)._2))
              //get current output
              getOutput(k, inStream.head)
            } //if comment that was liked has a graph, calculate
            else {
              //ALREADY LIKED BEFORE
              //add vertex to graph
              g.addVertex(user_id)
              var changed = false
              //if user that liked has friendship
              if (friendships.contains(user_id)) {
                //get all edges that should be in the graph after edge addition
                val edgesToAdd = friendships(user_id).filter(id => g.vertexExists(id))
                if (edgesToAdd.nonEmpty) {
                  g.addEdgesToVertex(user_id, edgesToAdd)
                  //update map with graphs
                  graphs += comment_id ->(g, g.range, timestamp)
                  changed = true
                }
                else {
                  graphs += comment_id ->(g, g.range, timestamp)
                }
              }
              else {
                graphs += comment_id ->(g, g.range, timestamp)
              }
              if (changed) {
                //get current output
                getOutput(k, inStream.head)
              }
              else {
                decayOutput(k, inStream.head)
              }
            }
            //update history of likes
            if (likes.contains(user_id)) {
              val adj = likes(user_id) ++ Set(comment_id)
              likes += user_id -> adj
            }
            else {
              likes += user_id -> Set(comment_id)
            }
          } //if input stream is a friendship
          else {
            val srcId = inStream(1).toLong
            val dstId = inStream(2).toLong
            if (friendships.contains(srcId)) {
              val adj1 = friendships(srcId) ++ Array(dstId)
              friendships += srcId -> adj1
            }
            else {
              friendships += srcId -> Array(dstId)
            }
            if (friendships.contains(dstId)) {
              val adj2 = friendships(dstId) ++ Array(srcId)
              friendships += dstId -> adj2
            }
            else {
              friendships += dstId -> Array(srcId)
            }
            var changed = false
            if (graphs.nonEmpty) {
              //start updating graphs
              if (likes.contains(srcId)) {
                val his = likes(srcId).filter(graphs.contains(_))
                if (his.nonEmpty) {
                  his.foreach { c =>
                    val g = graphs(c)._1
                    val timestamp = graphs(c)._3
                    if (g.vertexExists(srcId) && g.vertexExists(dstId)) {
                      g.addEdge(srcId, dstId)
                      graphs += c ->(g, g.range, timestamp)
                      changed = true
                    }
                  }
                }
              }
              if (likes.contains(dstId)) {
                val his = likes(dstId).filter(graphs.contains(_))
                if (his.nonEmpty) {
                  his.foreach { c =>
                    val g = graphs(c)._1
                    val timestamp = graphs(c)._3
                    if (g.vertexExists(srcId) && g.vertexExists(dstId)) {
                      g.addEdge(srcId, dstId)
                      graphs += c ->(g, g.range, timestamp)
                      changed = true
                    }
                  }
                }
              }
            }
            if (changed) {
              //get current output
              getOutput(k, inStream.head)
            }
            else {
              decayOutput(k, inStream.head)
              //getOutput(k, graphs, inStream.head)
            }
          }
        }
      }
    }
    log.write("*" + Calendar.getInstance().getTime + ": Q2:\tFinished processing file.")

    val end = System.currentTimeMillis*1000000/1000
    val latAvg = (latSum/latAmt).toDouble/1000000
    val performance = (end-start).toDouble/1000000
    val performanceStr = String.format("%.6f", new java.lang.Double(performance))
    val latAvgStr = String.format("%.6f", new java.lang.Double(latAvg))
    val perfOut = new FileWriter("performance.txt", true)
    perfOut.write("\n\nQ2a total duration:\t" + performanceStr + "\n")
    perfOut.write("Q2b average latency:\t" + latAvgStr + "\n")
    perfOut.close

    val w = new FileWriter("q2.txt")
    outArr.drop(1).foreach(w.write(_))
    w.close
  }

  private def decayOutput(k: Int, ts: String) = {
    var curOutput = ""
    if (sortedArray.nonEmpty) {
      val sortedFinal = sortedArray.take(k).map(x => x._3)
      var n = sortedFinal.length
      curOutput = sortedFinal.mkString(",")
      while (n < k) {
        curOutput += ",-"
        n += 1
      }
    }
    else {
      var i = 0
      var strings = ""
      while (i < k) {
        strings += ",-"
        i += 1
      }
      curOutput = strings.replaceFirst("[,]", "")
    }
    if (curOutput != lastOutput) {
      lastOutput = curOutput
      outArr ++= Array(ts + "," + curOutput + "\n")
    }
    val latEnd = System.currentTimeMillis*1000000/1000
    val latency = latEnd-latStart
    latSum += latency
    latAmt += 1
  }

  private def getOutput(k: Int, ts: String) = {
    var curOutput = ""
    if (sortedArray.nonEmpty) {
      sortedArray = sortedArray.map(x => x._1).distinct.map(x => (x, graphs(x)._2, comments(x)._2))
      sortedArray = sortedArray.sortWith((t1,t2) => if (t1._2 == t2._2) t1._3.toLowerCase < t2._3.toLowerCase else t1._2 > t2._2)
      val sortedFinal = sortedArray.take(k).map(x => x._3)
      var n = sortedFinal.length
      curOutput = sortedFinal.mkString(",")
      while (n < k) {
        curOutput += ",-"
        n += 1
      }
    }
    else {
      var i = 0
      var strings = ""
      while (i < k) {
        strings += ",-"
        i += 1
      }
      curOutput = strings.replaceFirst("[,]", "")
    }
    if (curOutput != lastOutput) {
      lastOutput = curOutput
      outArr ++= Array(ts + "," + curOutput + "\n")
    }
    val latEnd = System.currentTimeMillis*1000000/1000
    val latency = latEnd-latStart
    latSum += latency
    latAmt += 1
  }

  /**
   * Given a string containing time
   * return milliseconds from 1970
   * @param time currentTime
   * @return seconds
   */
  private def getMillis(time: String): Long = {
    val now = time.replaceAll("[T]", " ")
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    val date = df.parse(now)
    date.getTime
  }

}
