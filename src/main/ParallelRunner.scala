package main

import scala.actors.Actor

/**
  * Created by yannis on 22-Mar-16.
  */
class ParallelRunner(query: String, k: Int, d: Int, clockStart: Long) extends Actor {

  override def act(): Unit = {
    if (query == "q1") {
      val q1 = new Query1
      q1.run(clockStart)
    }
    else {
      val q2 = new Query2
      q2.run(k, d, clockStart)
    }
  }
}
