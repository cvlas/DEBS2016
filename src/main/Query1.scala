package main

import java.io.{FileOutputStream, PrintWriter, File}
import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.io.Source

/**
 * @author Vlassopoulos Christos
 */
class Query1 {

  // Commented: [Id, (Author, TS, LastActivityTS, [CommentIDs], [DistinctCommenters])]
  var commented: collection.mutable.Map[Long, (String, Long, Long, Array[Long], Set[String])] = collection.mutable.Map()
  // Commentless: [Id, (Author, TS, LastActivityTS, [CommentIDs], [DistinctCommenters])]
  var commentless: collection.mutable.Map[Long, (String, Long, Long, Array[Long], Set[String])] = collection.mutable.Map()
  // Comment: [Id, (TS, ParentPostID, LastTime)]
  var comments: collection.mutable.Map[Long, (Long, Long, String)] = collection.mutable.Map()

  var lastOutput = ""

  val MILLIS_PER_DAY: Long = 24*60*60*1000  // MILLISECONDS! NOT SECONDS!
  val MILLIS_TEN_DAYS: Long = 10*MILLIS_PER_DAY

  var top3 = Array.empty[(Long, Int, String, Long, Long, Int)]

  /**
   * Function that calculates how many days have elapsed since a certain timestamp.
   * Millisecond precision.
   * now:  current epoch,
   * ts:   timestamp of interest
   * @return number of days elapsed
   */
  val daysElapsed = (now: Long, ts: Long) => {
    ((now - ts).toDouble/MILLIS_PER_DAY).toInt
  }

  /**
   * Given a timestamp (String), return milliseconds from 1970
   * @param time currentTime
   * @return seconds
   */
  private def getMillis(time: String): Long = {
    val now = time.replaceAll("[T]", " ")
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    val date = df.parse(now)
    date.getTime
  }

  private def calculateCommentedScore(postId: Long, now: Long): (Long, Int, String, Long, Long, Int) = {
    val finalDecay = daysElapsed(now, commented(postId)._2)
    val newPostScore = {if ((10 - finalDecay) >= 0) (10 - finalDecay) else 0 }  // Non-negative number

    var totalCommentScore = 0

    commented(postId)._4.foreach{ commentId =>
      val cfinalDecay = daysElapsed(now, comments(commentId)._1)
      val newCommentScore = {if ((10 - cfinalDecay) >= 0) (10 - cfinalDecay) else 0 }   // Non-negative number
      if (newCommentScore == 0) {                                                                   // If commentScore = 0
      val updatedCommentArray = commented(postId)._4.filterNot(_ == commentId)                    // Remove comment from array
      val updatedCommenterSet = updatedCommentArray.map(id => comments(id)._3).toSet              // Remove commenter from commenters' Set, if necessary
      val newTuple1 = commented(postId).copy(_4 = updatedCommentArray, _5 = updatedCommenterSet)  // Update array & Update set
        commented += (postId -> newTuple1)
      }

      totalCommentScore += newCommentScore
    }

    val newTotalPostScore = newPostScore + totalCommentScore

    (postId, newTotalPostScore, commented(postId)._1, commented(postId)._2, commented(postId)._3, commented(postId)._5.size)
  }

  private def calculateCommentlessScore(postId: Long, now: Long): (Long, Int, String, Long, Long, Int) = {
    val finalDecay = daysElapsed(now, commentless(postId)._2)
    val newPostScore = {if ((10 - finalDecay) >= 0) (10 - finalDecay) else 0 }  // Non-negative number
    val totalCommentScore = 0
    val newTotalPostScore = newPostScore + totalCommentScore

    (postId, newTotalPostScore, commentless(postId)._1, commentless(postId)._2, commentless(postId)._3, commentless(postId)._5.size)
  }

  def run(clockStart: Long) = {
    val logOut = new PrintWriter(new FileOutputStream(new File("log.txt"), true))

    try {
      logOut.println("\n\n" + Calendar.getInstance().getTime + ": Q1:\tStarted.")
      var latSum = 0L
      var latAmt = 0
      var outArr = Array.empty[String]

      val start = clockStart / 1000

      for (line <- Source.fromFile("merged1.dat")("UTF-8").getLines) {
        if (line != "") {

          val latStart = System.currentTimeMillis * 1000
          val eventString = line //+ "$" // end every string with "$" so all comment events have 7 fields
          val event = eventString.split("[|]")
          val eventLength = event.length

          val nowts = event(0) // head
          val now = getMillis(nowts)


          var dyingTop3 = top3.filter(_._5 + MILLIS_TEN_DAYS < now)

          while (dyingTop3.nonEmpty) {
            val deathTimeTop3 = dyingTop3.map(x => x._5 + MILLIS_TEN_DAYS).sortWith((t1, t2) => t1 < t2)

            for (deathTs <- deathTimeTop3) {
              val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
              val gmtTime = TimeZone.getTimeZone("GMT")
              df.setTimeZone(gmtTime)
              val cal = Calendar.getInstance()
              cal.setTimeInMillis(deathTs)
              val deathDate = df.format(cal.getTime).replaceAll(" ", "T")

              top3 = Array.empty[(Long, Int, String, Long, Long, Int)]

              if (!commented.isEmpty) {
                commented.foreach { post =>

                  val tuple = calculateCommentedScore(post._1, deathTs)

                  if (tuple._2 > 0) {

                    // Sort based on score and then on the timestamps
                    if (top3.length < 3 || tuple._2 >= top3.last._2) {
                      top3 :+= tuple
                      top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                        if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                      } else t1._2 > t2._2).take(3)
                    }

                  }
                  else {
                    commented -= post._1 // Remove it
                  }
                }

                if (top3.length < 3 || top3.last._2 <= 10) {
                  commentless.foreach { post =>

                    val tuple = calculateCommentlessScore(post._1, deathTs)

                    if (tuple._2 > 0) {

                      // Sort based on score and then on the timestamps
                      if (top3.length < 3 || tuple._2 >= top3.last._2) {
                        top3 :+= tuple
                        top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                          if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                        } else t1._2 > t2._2).take(3)
                      }
                    }
                    else {
                      commentless -= post._1 // Remove it
                    }
                  }
                }
              }
              else {
                commentless.foreach { post =>

                  val tuple = calculateCommentlessScore(post._1, deathTs)

                  if (tuple._2 > 0) {

                    // Sort based on score and then on the timestamps
                    if (top3.length < 3 || tuple._2 >= top3.last._2) {
                      top3 :+= tuple
                      top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                        if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                      } else t1._2 > t2._2).take(3)
                    }

                  }
                  else {
                    commentless -= post._1 // Remove it
                  }
                }
              }

              val sorted = top3.map(z => (z._1, z._3, z._2, z._6))
              var n = sorted.length

              var outputStr = (if (n == 0) "" else ",") + sorted.mkString(",").replaceAll("[()]", "")

              while (n < 3) {
                outputStr += ",-,-,-,-"
                n += 1
              }

              val parts = outputStr.split("[,]")
              val newOutput = parts(1) + "," + parts(5) + "," + parts(9)
              //        val latEnd = System.currentTimeMillis*1000000 / 1000
              //        val latency = latEnd - latStart
              //        latSum += latency
              //        latAmt += 1
              if (lastOutput != newOutput) {
                lastOutput = newOutput
                println(deathDate + outputStr)
                outArr ++= Array(deathDate + outputStr)
              }
              else {
              }
            }

            dyingTop3 = top3.filter(_._5 + MILLIS_TEN_DAYS <= now)
          }

          if (eventLength == 7) {

            // we have a comment
            val commentId = event(1)
            val commenterName = event(4)
            val postReplied = event(6)
            var parentPostId = 0L

            if (postReplied equals "-1") {
              // we have a comment chain
              val commentRepliedId = event(5).toLong // find the replied commentId
              if (comments contains commentRepliedId)
                parentPostId = comments(commentRepliedId)._2 // retrieve that Long value from comments Map
            }
            else {
              // direct reply to a post
              parentPostId = postReplied.toLong // get postId directly from the data
            }

            if (((commented contains parentPostId) && (calculateCommentedScore(parentPostId, now)._2 != 0)) || ((commentless contains parentPostId) && (calculateCommentlessScore(parentPostId, now)._2 != 0))) {
              // Zombie/Stillborn-proof
              if (commentless contains parentPostId) {
                val tuple = commentless(parentPostId)
                commented += (parentPostId -> tuple)

                commentless -= parentPostId

              }

              // ONLY if post is active (ie it has not been deleted from the posts Map nor has it been stillborn)
              comments += (commentId.toLong ->(now, parentPostId, commenterName)) // Add comment to comments Map
              val updatedCommentArray = commented(parentPostId)._4 :+ commentId.toLong // Append commentId to commentIds array
              commented(parentPostId) = commented(parentPostId).copy(_4 = updatedCommentArray) // Update array

              if (!commenterName.equals(commented(parentPostId)._1)) {
                // If the commenter is not the author of the post.
                val updatedCommenterSet = commented(parentPostId)._5 + commenterName // Add commenter name to the Set of commenters
                commented(parentPostId) = commented(parentPostId).copy(_5 = updatedCommenterSet) // Update Set
              }

              commented(parentPostId) = commented(parentPostId).copy(_3 = now) // Ultimately, update Last Activity Timestamp
            }
          }
          else if (eventLength == 5) {

            // If we have a post
            val postId = event(1).toLong
            val authorName = event(4) //.stripSuffix("$")

            commentless += (postId ->(authorName, now, now, Array[Long](), Set[String]()))
          }

          if (!commented.isEmpty) {

            if (top3.nonEmpty) {
              top3 = top3.map{p =>
                if (commentless.contains(p._1)) {
                  calculateCommentlessScore(p._1,now)
                }
                else
                  calculateCommentedScore(p._1,now)
              }
                  .sortWith((t1, t2) => if (t1._2 == t2._2) {
                if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
              } else t1._2 > t2._2)
            }

            top3 = top3.filterNot(p => p._2 == 0)

            val topIds = top3.map(_._1)

            val parCommented = commented.par
            parCommented.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))

            parCommented.foreach { post =>
              val tuple = calculateCommentedScore(post._1, now)

              if (tuple._2 > 0) {

                synchronized{
                  if (top3.length < 3 )
                  {
                    if (!topIds.contains(post._1)) {
                      top3 :+= tuple
                    }
                  }
                  else if (top3.last._2 <= tuple._2) {
                    if (!topIds.contains(post._1)) {
                      top3 :+= tuple
                    }
                  }
                }
              }
              else {
                commented -= post._1 // Remove it
              }
            }

            top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
              if (t1._4 == t2._4) { if (t1._5 == t2._5) t1._1 > t2._1 else  t1._5 > t2._5 } else t1._4 > t2._4
            } else t1._2 > t2._2).take(3)

            if (top3.length < 3 || top3.last._2 <= 10) {

              if (top3.nonEmpty) {
                top3 = top3.map{p =>
                  if (commentless.contains(p._1)) {
                    calculateCommentlessScore(p._1,now)
                  }
                  else
                    calculateCommentedScore(p._1,now)
                }
                    .sortWith((t1, t2) => if (t1._2 == t2._2) {
                  if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                } else t1._2 > t2._2)
              }

              top3 = top3.filterNot(p => p._2 == 0)

              val topIds = top3.map(_._1)

              val parCommentless = commentless.par
              parCommentless.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))

              parCommentless.foreach { post =>
                val tuple = calculateCommentlessScore(post._1, now)

                if (tuple._2 > 0) {

                  synchronized{
                    if (top3.length < 3 )
                    {
                      if (!topIds.contains(post._1)) top3 :+= tuple
                    }
                    else if (top3.last._2 <= tuple._2) {
                      if (!topIds.contains(post._1)) top3 :+= tuple
                    }
                  }

                }
                else {
                  commentless -= post._1 // Remove it
                }
              }
              top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                if (t1._4 == t2._4) { if (t1._5 == t2._5) t1._1 > t2._1 else  t1._5 > t2._5 } else t1._4 > t2._4
              } else t1._2 > t2._2).take(3)
            }
          }
          else {

            if (top3.nonEmpty) {
              top3 = top3.map{p =>
                if (commentless.contains(p._1)) {
                  calculateCommentlessScore(p._1,now)
                }
                else
                  calculateCommentedScore(p._1,now)
              }.sortWith((t1, t2) => if (t1._2 == t2._2) {
                if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
              } else t1._2 > t2._2)
            }

            top3 = top3.filterNot(p => p._2 == 0)

            val topIds = top3.map(_._1)

            val parCommentless = commentless.par
            parCommentless.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))

            parCommentless.foreach { post =>
              val tuple = calculateCommentlessScore(post._1, now)

              if (tuple._2 > 0) {

                synchronized{
                  if (top3.length < 3 )
                  {
                    if (!topIds.contains(post._1)) top3 :+= tuple
                  }
                  else if (top3.last._2 <= tuple._2) {
                    if (!topIds.contains(post._1)) top3 :+= tuple
                  }
                }

              }
              else {
                commentless -= post._1 // Remove it
              }
            }
            top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
              if (t1._4 == t2._4) { if (t1._5 == t2._5) t1._1 > t2._1 else  t1._5 > t2._5 } else t1._4 > t2._4
            } else t1._2 > t2._2).take(3)
          }

          val sorted = top3.map(z => (z._1, z._3, z._2, z._6))
          var n = sorted.length

          //        if (n > 0) {
          var outputStr = (if (n == 0) "" else ",") + sorted.mkString(",").replaceAll("[()]", "")

          while (n < 3) {
            outputStr += ",-,-,-,-"
            n += 1
          }

          val parts = outputStr.split("[,]")
          val newOutput = parts(1) + "," + parts(5) + "," + parts(9)
          val latEnd = System.currentTimeMillis * 1000000 / 1000
          val latency = latEnd - latStart
          latSum += latency
          latAmt += 1
          if (lastOutput != newOutput) {
            lastOutput = newOutput
            println(nowts + outputStr)
            outArr ++= Array(nowts + outputStr)
          }
          else {
          }
        }
      }

      var deathTop3 = top3.map(x => x._5 + MILLIS_TEN_DAYS).sortWith((t1, t2) => t1 < t2)

      while (deathTop3.nonEmpty) {
        //        for (deathTs <- deathTop3) {
        val deathTs = deathTop3.head

        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
        val gmtTime = TimeZone.getTimeZone("GMT")
        df.setTimeZone(gmtTime)
        val cal = Calendar.getInstance()
        cal.setTimeInMillis(deathTs)
        val deathDate = df.format(cal.getTime).replaceAll(" ", "T")

        top3 = Array.empty[(Long, Int, String, Long, Long, Int)]

        if (!commented.isEmpty) {
          commented.foreach { post =>

            val tuple = calculateCommentedScore(post._1, deathTs)

            if (tuple._2 > 0) {

              // Sort based on score and then on the timestamps
              if (top3.length < 3 || tuple._2 >= top3.last._2) {
                top3 :+= tuple
                top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                  if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                } else t1._2 > t2._2).take(3)
              }

            }
            else {
              commented -= post._1    // Remove it
            }
          }

          if (top3.length < 3 || top3.last._2 <= 10) {
            commentless.foreach { post =>

              val tuple = calculateCommentlessScore(post._1, deathTs)

              if (tuple._2 > 0) {

                // Sort based on score and then on the timestamps
                if (top3.length < 3 || tuple._2 >= top3.last._2) {
                  top3 :+= tuple
                  top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                    if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                  } else t1._2 > t2._2).take(3)
                }

              }
              else {
                commentless -= post._1    // Remove it
              }
            }
          }
        }
        else {
          commentless.foreach { post =>

            val tuple = calculateCommentlessScore(post._1, deathTs)

            if (tuple._2 > 0) {

              // Sort based on score and then on the timestamps
              if (top3.length < 3 || tuple._2 >= top3.last._2) {
                top3 :+= tuple
                top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                  if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                } else t1._2 > t2._2).take(3)
              }

            }
            else {
              commentless -= post._1    // Remove it
            }
          }
        }

        val sorted = top3.map(z => (z._1, z._3, z._2, z._6))
        var n = sorted.length

        //        if (n > 0) {
        var outputStr = (if (n == 0) "" else ",") + sorted.mkString(",").replaceAll("[()]", "")

        while (n < 3) {
          outputStr += ",-,-,-,-"
          n += 1
        }

        val parts = outputStr.split("[,]")
        val newOutput = parts(1) + "," + parts(5) + "," + parts(9)
        //        val latEnd = System.currentTimeMillis*1000000 / 1000
        //        val latency = latEnd - latStart
        //        latSum += latency
        //        latAmt += 1
        if (lastOutput != newOutput) {
          lastOutput = newOutput
          println(deathDate + outputStr)
          outArr ++= Array(deathDate + outputStr)
        }
        else {
        }
        //        }

        deathTop3 = top3.map(x => x._5 + MILLIS_TEN_DAYS).sortWith((t1, t2) => t1 < t2)
      }

      logOut.println(Calendar.getInstance().getTime + ": Q1:\tFinished processing file.")

      val end = System.currentTimeMillis*1000000 / 1000

      val latAvg = (latSum / latAmt).toDouble / 1000000
      val performance = (end - start).toDouble / 1000000

      val perfOut = new PrintWriter(new FileOutputStream(new File("performance.txt"), true))
      perfOut.printf("Q1a total duration:\t%.6f", new java.lang.Double(performance))
      perfOut.printf("\nQ1b average latency:\t%.6f", new java.lang.Double(latAvg))
      perfOut.close()

      val resultsOut = new PrintWriter(new FileOutputStream(new File("q1.txt"), true))
      if (outArr.head.endsWith(",-,-,-,-,-,-,-,-,-,-,-,-")) {
        outArr = outArr.drop(1)
      }
      outArr.foreach(resultsOut.println(_))
      resultsOut.close()
    }
    catch {
      case e: Exception => logOut.println("Q1 (Exception):\t" + e.toString + "\n\n" + e.getStackTraceString)
    }
    finally {
      logOut.close()
    }
  }
}
