package main

import java.io.{FileOutputStream, PrintWriter, File}
import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.io.Source

/**
 * Created by Christos on 11/4/2016.
 */
class Query1SuperFast {

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
   *
   * @return number of days elapsed
   */
  val daysElapsed = (now: Long, ts: Long) => {
    //    println("DAYSELAPSED: Now: " + now)
    //    println("DAYSELAPSED: Post's TS: " + ts)
    //    println((now - ts).toDouble/MILLIS_PER_DAY)
    //    println((math rint (now - ts).toDouble/MILLIS_PER_DAY).toInt)
    ((now - ts).toDouble/MILLIS_PER_DAY).toInt
  }

  /**
   * Given a timestamp (String), return milliseconds from 1970
   *
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
    //println("CALCSCORE: Final Decay: " + finalDecay)
    val newPostScore = {if ((10 - finalDecay) >= 0) (10 - finalDecay) else 0 }  // Non-negative number
    //println("CALCSCORE: Post with id " + postId + " will have a new post score of " + newPostScore)

    var totalCommentScore = 0

    commented(postId)._4.foreach{ commentId =>
      val cfinalDecay = daysElapsed(now, comments(commentId)._1)
      //println("CALCSCORE: Comment final Decay: " + cfinalDecay)
      val newCommentScore = {if ((10 - cfinalDecay) >= 0) (10 - cfinalDecay) else 0 }   // Non-negative number
      //println("CALCSCORE: Comment with id " + commentId + " will have a new comment score of " + newCommentScore)

      if (newCommentScore == 0) {                                                                   // If commentScore = 0
      val updatedCommentArray = commented(postId)._4.filterNot(_ == commentId)                    // Remove comment from array
      //println(updatedCommentArray.deep)
      val updatedCommenterSet = updatedCommentArray.map(id => comments(id)._3).toSet              // Remove commenter from commenters' Set, if necessary
      //println(updatedCommenterSet)
      val newTuple1 = commented(postId).copy(_4 = updatedCommentArray, _5 = updatedCommenterSet)  // Update array & Update set
        commented += (postId -> newTuple1)
      }

      totalCommentScore += newCommentScore
    }

    //println("CALCSCORE: Post with id " + postId + " will have a new comment score of " + totalCommentScore)

    val newTotalPostScore = newPostScore + totalCommentScore

    (postId, newTotalPostScore, commented(postId)._1, commented(postId)._2, commented(postId)._3, commented(postId)._5.size)
  }

  private def calculateCommentlessScore(postId: Long, now: Long): (Long, Int, String, Long, Long, Int) = {
    val finalDecay = daysElapsed(now, commentless(postId)._2)
    //println("CLCALCSCORE: Final Decay: " + finalDecay)
    val newPostScore = {if ((10 - finalDecay) >= 0) (10 - finalDecay) else 0 }  // Non-negative number
    //println("CLCALCSCORE: Post with id " + postId + " will have a new post score of " + newPostScore)
    val totalCommentScore = 0
    //println("CLCALCSCORE: Post with id " + postId + " will have a new comment score of " + totalCommentScore)
    val newTotalPostScore = newPostScore + totalCommentScore

    (postId, newTotalPostScore, commentless(postId)._1, commentless(postId)._2, commentless(postId)._3, commentless(postId)._5.size)
  }

  def run(clockStart: Long) = {
    val logOut = new PrintWriter(new FileOutputStream(new File("log.txt"), true))
    //    println("Q1: Initiated log...")
    //    logOut.println("Q1: Initiated log...\nLog is responding...")

    try {
      logOut.println("\n\n" + Calendar.getInstance().getTime + ": Q1:\tStarted.")
      var latSum = 0L
      var latAmt = 0
      var outArr = Array.empty[String]

      val start = clockStart / 1000

      //      logOut.println("ClockStart in ns: " + clockStart)
      //      logOut.println("ClockStart in us: " + start)

      for (line <- Source.fromFile("merged1.dat")("UTF-8").getLines) {
        if (line != "") {

          //println("ELABA: " + line)
          val latStart = System.currentTimeMillis * 1000000 / 1000
          val eventString = line //+ "$" // end every string with "$" so all comment events have 7 fields
          val event = eventString.split("[|]")
          val eventLength = event.length
          // println(event.deep + "\t" + eventLength)
          val nowts = event(0) // head
          val now = getMillis(nowts)

          // println("Top3 is " + top3.deep)

          var dyingTop3 = top3.filter(_._5 + MILLIS_TEN_DAYS < now)

          //println("DyingTop3 is " + dyingTop3.deep)

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
              //println("EMPTIED TOP3")

              if (!commented.isEmpty) {
                commented.foreach { post =>
                  //    println("FOR: Post " + post)

                  val tuple = calculateCommentedScore(post._1, deathTs)

                  //     println("FOR: has a total score of " + tuple._2)
                  //println("FOR: is stored in the Map with a score of " + commented(post._1)._8)

                  if (tuple._2 > 0) {

                    //   println("FOR: Currently top3 is: " + top3.deep)

                    // Sort based on score and then on the timestamps
                    if (top3.length < 3 || tuple._2 >= top3.last._2) {
                      top3 :+= tuple
                      top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                        if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                      } else t1._2 > t2._2).take(3)
                    }

                    //    println("FOR: New top3 is: " + top3.deep)
                  }
                  else {
                    //      println("FOR: TO BE REMOVED: " + commented(post._1))
                    commented -= post._1 // Remove it
                  }
                }

                if (top3.length < 3 || top3.last._2 <= 10) {
                  commentless.foreach { post =>
                    //       println("CLFOR: Post " + post)

                    val tuple = calculateCommentlessScore(post._1, deathTs)

                    //    println("CLFOR: has a total score of " + tuple._2)
                    //              println("CLFOR: is stored in the Map with a score of " + commentless(post._1)._8)

                    if (tuple._2 > 0) {

                      //           println("CLFOR: Currently top3 is: " + top3.deep)

                      // Sort based on score and then on the timestamps
                      if (top3.length < 3 || tuple._2 >= top3.last._2) {
                        top3 :+= tuple
                        top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                          if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                        } else t1._2 > t2._2).take(3)
                      }

                      //       println("CLFOR: New top3 is: " + top3.deep)
                    }
                    else {
                      //       println("FOR: TO BE REMOVED: " + commentless(post._1))
                      commentless -= post._1 // Remove it
                    }
                  }
                }
              }
              else {
                commentless.foreach { post =>
                  //  println("CLFOR: Post " + post)

                  val tuple = calculateCommentlessScore(post._1, deathTs)

                  //   println("CLFOR: has a total score of " + tuple._2)
                  //            println("CLFOR: is stored in the Map with a score of " + commentless(post._1)._8)

                  if (tuple._2 > 0) {

                    //      println("CLFOR: Currently top3 is: " + top3.deep)

                    // Sort based on score and then on the timestamps
                    if (top3.length < 3 || tuple._2 >= top3.last._2) {
                      top3 :+= tuple
                      top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                        if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                      } else t1._2 > t2._2).take(3)
                    }

                    //     println("CLFOR: New top3 is: " + top3.deep)
                  }
                  else {
                    //      println("CLFOR: TO BE REMOVED: " + commentless(post._1))
                    commentless -= post._1 // Remove it
                  }
                }
              }

              val sorted = top3.map(z => (z._1, z._3, z._2, z._6))
              //  println("Output: " + sorted.deep)
              var n = sorted.length

              //        if (n > 0) {
              var outputStr = (if (n == 0) "" else ",") + sorted.mkString(",").replaceAll("[()]", "")

              while (n < 3) {
                outputStr += ",-,-,-,-"
                n += 1
              }

              val parts = outputStr.split("[,]")
              //  println("OMG THAT IS SO DEEP: " + parts.deep)
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
                //    println("WILL NOT PRODUCE OUTPUT: " + lastOutput + "==" + newOutput)
              }
            }

            dyingTop3 = top3.filter(_._5 + MILLIS_TEN_DAYS <= now)
          }

          if (eventLength == 7) {
            //   println("READCOMM: Il s'agit d'un comment")
            //          val readcomment_start = System.nanoTime()

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
                //        println("READCOMM: COMMENTED: " + commented)
                commentless -= parentPostId
                //        println("READCOMM: COMMENTLESS: " + commentless)
              }

              //            println("I decided to enter this IF statement, because post " + parentPostId + "'s score is " + posts(parentPostId)._8 + " and this is obviously not 0.")
              //            println("Also, this post's commenter set has a size of " + posts(parentPostId)._6.size + " which is obviously not 0.")
              // ONLY if post is active (ie it has not been deleted from the posts Map nor has it been stillborn)
              comments += (commentId.toLong ->(now, parentPostId, commenterName)) // Add comment to comments Map
              val updatedCommentArray = commented(parentPostId)._4 :+ commentId.toLong // Append commentId to commentIds array
              commented(parentPostId) = commented(parentPostId).copy(_4 = updatedCommentArray) // Update array

              //         println("READCOMM: Post with Id " + parentPostId + " is in the commented map and has " + commented(parentPostId)._4.length + " comments, namely:\n" + commented(parentPostId)._4.deep)

              if (!commenterName.equals(commented(parentPostId)._1)) {
                // If the commenter is not the author of the post.
                val updatedCommenterSet = commented(parentPostId)._5 + commenterName // Add commenter name to the Set of commenters
                commented(parentPostId) = commented(parentPostId).copy(_5 = updatedCommenterSet) // Update Set
              }

              commented(parentPostId) = commented(parentPostId).copy(_3 = now) // Ultimately, update Last Activity Timestamp
            }
          }
          else if (eventLength == 5) {
            //          println("READPOST: Il s'agit d'un post")

            // If we have a post
            val postId = event(1).toLong
            val authorName = event(4) //.stripSuffix("$")

            commentless += (postId ->(authorName, now, now, Array[Long](), Set[String]()))
            //println("READPOST: COMMENTLESS: " + commentless)
          }

          //          top3 = Array.empty[(Long, Int, String, Long, Long, Int)]

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
              //   println("FOR: Post " + post)
//              Thread.sleep(1)
              //println("PARFOR: Post " + post._1 + " has " + post._2._5.size + " commenters.")
              val tuple = calculateCommentedScore(post._1, now)
              //println("PARFOR: Tuple " + tuple._1 + " has " + tuple._6 + " commenters.")

              //    println("FOR: has a total score of " + tuple._2)
              //            println("FOR: is stored in the Map with a score of " + commented(post._1)._8)

              if (tuple._2 > 0) {

                synchronized{
                  if (top3.length < 3 )
                  {
                    if (!topIds.contains(post._1)) {
                      top3 :+= tuple
                      // println("FOR: Added " + tuple + " to top3")

                      //println("FOR: Currently top3 is: " + top3.deep)
                    }
                  }
                  else if (top3.last._2 <= tuple._2) {
                    if (!topIds.contains(post._1)) {
                      top3 :+= tuple
                      //println("FOR: Added " + tuple + " to top3")

                      //println("FOR: Currently top3 is: " + top3.deep)
                    }
                  }
                }

                //        println("FOR: Katw Currently top3 is: " + top3.deep)
              }
              else {
                //       println("FOR: TO BE REMOVED: " + commented(post._1))
                commented -= post._1 // Remove it
              }
            }

            //println("Before sort" + top3.deep)
            top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
              if (t1._4 == t2._4) { if (t1._5 == t2._5) t1._1 > t2._1 else  t1._5 > t2._5 } else t1._4 > t2._4
            } else t1._2 > t2._2).take(3)
            //println("After sort" + top3.deep)

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
                //                  println("BIKA")
                //    println("CLFOR: Post " + post)
//                Thread.sleep(1)
                val tuple = calculateCommentlessScore(post._1, now)

                //    println("CLFOR: has a total score of " + tuple._2)
                //              println("CLFOR: is stored in the Map with a score of " + commentless(post._1)._8)

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

                  //     println("CLFOR: Currently top3 is: " + top3.deep)
                }
                else {
                  //    println("FOR: TO BE REMOVED: " + commentless(post._1))
                  commentless -= post._1 // Remove it
                }
              }
              // println("Before sort" + top3.deep)
              top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                if (t1._4 == t2._4) { if (t1._5 == t2._5) t1._1 > t2._1 else  t1._5 > t2._5 } else t1._4 > t2._4
              } else t1._2 > t2._2).take(3)
              //println("After sort" + top3.deep)
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
              //              println("BGIKA")
              //println("CLFOR: Post " + post)
//              Thread.sleep(1)
              val tuple = calculateCommentlessScore(post._1, now)

              //println("CLFOR: has a total score of " + tuple._2)
              //            println("CLFOR: is stored in the Map with a score of " + commentless(post._1)._8)

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

                //println("CLFOR: Currently top3 is: " + top3.deep)
              }
              else {
                // println("CLFOR: TO BE REMOVED: " + commentless(post._1))
                commentless -= post._1 // Remove it
              }
            }
            //println("Before sort" + top3.deep)
            top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
              if (t1._4 == t2._4) { if (t1._5 == t2._5) t1._1 > t2._1 else  t1._5 > t2._5 } else t1._4 > t2._4
            } else t1._2 > t2._2).take(3)
            //println("After sort" + top3.deep)
          }

          val sorted = top3.map(z => (z._1, z._3, z._2, z._6))
          //println("Output: " + sorted.deep)
          var n = sorted.length

          //        if (n > 0) {
          var outputStr = (if (n == 0) "" else ",") + sorted.mkString(",").replaceAll("[()]", "")

          while (n < 3) {
            outputStr += ",-,-,-,-"
            n += 1
          }

          val parts = outputStr.split("[,]")
          //println("OMG THAT IS SO DEEP: " + parts.deep)
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
            //println("WILL NOT PRODUCE OUTPUT: " + lastOutput + "==" + newOutput)
          }
          //        }

          //        printf("Event No: %d. Event Processing: %d [ReadComment: %d, ReadPost: %d, ForLoop: %d [CalcScore: %d, Sorting: %d]]\n", eventcntr, total/eventcntr, readcomment/eventcntr, readpost/eventcntr, forloop/eventcntr, calcscore/eventcntr, sorting/eventcntr)
        }
      }
      //      printf("Events: %d. Event Processing: %d [ReadComment: %d, ReadPost: %d, ForLoop: %d [CalcScore: %d, Sorting: %d]]\n", eventcntr, total/eventcntr, readcomment/eventcntr, readpost/eventcntr, forloop/eventcntr, calcscore/eventcntr, sorting/eventcntr)

      var deathTop3 = top3.map(x => x._5 + MILLIS_TEN_DAYS).sortWith((t1, t2) => t1 < t2)

      while (deathTop3.nonEmpty) {
        //        for (deathTs <- deathTop3) {
        val deathTs = deathTop3.head
        //        println("BIKA")

        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
        val gmtTime = TimeZone.getTimeZone("GMT")
        df.setTimeZone(gmtTime)
        val cal = Calendar.getInstance()
        cal.setTimeInMillis(deathTs)
        val deathDate = df.format(cal.getTime).replaceAll(" ", "T")

        top3 = Array.empty[(Long, Int, String, Long, Long, Int)]
        //println("EMPTIED TOP3!!!")

        if (!commented.isEmpty) {
          commented.foreach { post =>
            //println("FOR: Post " + post)

            val tuple = calculateCommentedScore(post._1, deathTs)

            // println("FOR: has a total score of " + tuple._2)
            //            println("FOR: is stored in the Map with a score of " + commented(post._1)._8)

            if (tuple._2 > 0) {

              //      println("FOR: Currently top3 is: " + top3.deep)

              // Sort based on score and then on the timestamps
              if (top3.length < 3 || tuple._2 >= top3.last._2) {
                top3 :+= tuple
                top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                  if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                } else t1._2 > t2._2).take(3)
              }

              //println("FOR: New top3 is: " + top3.deep)
            }
            else {
              //println("FOR: TO BE REMOVED: " + commented(post._1))
              commented -= post._1    // Remove it
            }
          }

          if (top3.length < 3 || top3.last._2 <= 10) {
            commentless.foreach { post =>
              // println("CLFOR: Post " + post)

              val tuple = calculateCommentlessScore(post._1, deathTs)

              //println("CLFOR: has a total score of " + tuple._2)
              //              println("CLFOR: is stored in the Map with a score of " + commentless(post._1)._8)

              if (tuple._2 > 0) {

                //println("CLFOR: Currently top3 is: " + top3.deep)

                // Sort based on score and then on the timestamps
                if (top3.length < 3 || tuple._2 >= top3.last._2) {
                  top3 :+= tuple
                  top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                    if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                  } else t1._2 > t2._2).take(3)
                }

                //println("CLFOR: New top3 is: " + top3.deep)
              }
              else {
                //println("FOR: TO BE REMOVED: " + commentless(post._1))
                commentless -= post._1    // Remove it
              }
            }
          }
        }
        else {
          commentless.foreach { post =>
            //println("CLFOR: Post " + post)

            val tuple = calculateCommentlessScore(post._1, deathTs)

            //println("CLFOR: has a total score of " + tuple._2)
            //            println("CLFOR: is stored in the Map with a score of " + commentless(post._1)._8)

            if (tuple._2 > 0) {

              //println("CLFOR: Currently top3 is: " + top3.deep)

              // Sort based on score and then on the timestamps
              if (top3.length < 3 || tuple._2 >= top3.last._2) {
                top3 :+= tuple
                top3 = top3.sortWith((t1, t2) => if (t1._2 == t2._2) {
                  if (t1._4 == t2._4) t1._5 > t2._5 else t1._4 > t2._4
                } else t1._2 > t2._2).take(3)
              }

              //println("CLFOR: New top3 is: " + top3.deep)
            }
            else {
              //println("CLFOR: TO BE REMOVED: " + commentless(post._1))
              commentless -= post._1    // Remove it
            }
          }
        }

        val sorted = top3.map(z => (z._1, z._3, z._2, z._6))
        //println("Output: " + sorted.deep)
        var n = sorted.length

        //        if (n > 0) {
        var outputStr = (if (n == 0) "" else ",") + sorted.mkString(",").replaceAll("[()]", "")

        while (n < 3) {
          outputStr += ",-,-,-,-"
          n += 1
        }

        val parts = outputStr.split("[,]")
        //println("OMG THAT IS SO DEEP: " + parts.deep)
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
          //println("WILL NOT PRODUCE OUTPUT: " + lastOutput + "==" + newOutput)
        }
        //        }

        deathTop3 = top3.map(x => x._5 + MILLIS_TEN_DAYS).sortWith((t1, t2) => t1 < t2)
      }

      logOut.println(Calendar.getInstance().getTime + ": Q1:\tFinished processing file.")

      val end = System.currentTimeMillis*1000000 / 1000

      //logOut.println("ClockEnd in ns: " + end * 1000)
      //logOut.println("ClockEnd in us: " + end)

      val latAvg = (latSum / latAmt).toDouble / 1000000
      val performance = (end - start).toDouble / 1000000

      //      logOut.println("Performance in us: " + (end - start))
      //      logOut.println("Performance in s: " + performance)

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
