package main

/**
 * Created by yannis on 12-Mar-16.
 */
object Run {

  def main(args: Array[String]): Unit = {

    //    val q1_alt = new Query1Alt
    //    q1_alt.run(System.currentTimeMillis*1000000)
    //    val q1_fast = new Query1Fast
    //    q1_fast.run(System.currentTimeMillis*1000000)
    //    val q1_sfast = new Query1SuperFast
    //    q1_sfast.run(System.currentTimeMillis*1000000)
    /*val q1 = new Query1
    q1.run(0L)

    val q2 = new Query2
    q2.run(3, 3600)*/

    val q1 = new ParallelRunner("q1", 0, 0, System.currentTimeMillis()*1000000.toLong)
    val q2 = new ParallelRunner("q2", args(0).toInt, args(1).toInt, System.currentTimeMillis()*1000000.toLong)
    q2.start()
    q1.start()
  }
}
