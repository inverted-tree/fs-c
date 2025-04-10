package de.pc2.dedup

import de.pc2.dedup.util.Log

object Main extends Log {
    def main(args: Array[String]): Unit = {
        println("Hello from Scala!")
        if (args.nonEmpty) {
            println(s"Arguments received: ${args.mkString(", ")}")
        }
    }
}
