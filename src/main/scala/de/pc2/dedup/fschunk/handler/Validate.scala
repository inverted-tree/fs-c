package de.pc2.dedup.fschunk.handler

import scopt.OParser

import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

/** Handler to validate a trace file
  */
class ValidateHandler() extends Reporting with FileDataHandler with Log {
    var totalFileSize = 0L
    var totalFileCount = 0L
    var totalChunkCount = 0L
    val startTime: Long = System.currentTimeMillis()

    logger.debug("Start")

    override def report(): Unit = {
        val secs = ((System.currentTimeMillis() - startTime) / 1000)
        if (secs > 0) {
            val mbs = totalFileSize / secs
            val fps = totalFileCount / secs
            logger.info(
              "File Count: %d (%d f/s), File Size %s (%s/s), Chunk Count: %d"
                  .format(
                    totalFileCount,
                    fps,
                    StorageUnit(totalFileSize),
                    StorageUnit(mbs),
                    totalChunkCount
                  )
            )
        }
    }

    def handle(fp: FilePart): Unit = {
        logger.debug("Validate file %s (partial)".format(fp.filename))
        totalChunkCount += fp.chunks.size
    }

    def handle(f: File): Unit = {
        logger.debug(
          "Validate file %s, chunks %s".format(f.filename, f.chunks.size)
        )
        totalFileSize += f.fileSize
        totalFileCount += 1
        totalChunkCount += f.chunks.size
    }

    override def quit(): Unit = {
        report()
        logger.debug("Exit")

    }
}

case class ValidationConfig(
    format: String = "protobuf",
    reportInterval: Int = 60,
    filenames: Seq[String] = Seq()
)

object Validate {
    def main(args: Array[String]): Unit = {
        val builder = OParser.builder[ValidationConfig]
        val parser = {
            import builder._
            OParser.sequence(
              programName("fs-c validate"),
              head("fs-c", "0.4.0"),
              opt[String]("format")
                  .valueName("<trace file format>")
                  .action((x, c) =>
                      if (Format.isFormat(x)) {
                          c.copy(x)
                      } else {
                          println("Invalid fsf-c file format")
                          sys.exit(1)
                      }
                  )
                  .text("Trace file format"),
              opt[Int]('r', "report")
                  .valueName("<seconds>")
                  .action((x, c) => c.copy(reportInterval = x))
                  .text("Interval between progress reports (default = 60"),
              arg[Seq[String]]("Input Files")
                  .valueName("<file1>,<file2>,...")
                  .action((x, c) => c.copy(filenames = x))
                  .text("Trace files to be parsed")
            )
        }

        val config: ValidationConfig =
            OParser.parse(parser, args, ValidationConfig()) match {
                case Some(c) => c
                case _       => sys.exit(1)
            }

        for (file <- config.filenames) {
            val handler = new ValidateHandler()
            val reader =
                Format(config.format).createReader(file, handler)
            val reporter = new Reporter(handler, config.reportInterval).start()

            reader.parse()
            reporter.quit()
            handler.quit()
        }
    }
}
