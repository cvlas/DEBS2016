package main

/**
 * @author Kontopoulos Ioannis
 */
object Run {

  def main(args: Array[String]): Unit = {

    val q1 = new ParallelRunner("q1", 0, 0, System.currentTimeMillis()*1000000.toLong)
    val q2 = new ParallelRunner("q2", args(0).toInt, args(1).toInt, System.currentTimeMillis()*1000000.toLong)
    q2.start()
    q1.start()

  }
}
