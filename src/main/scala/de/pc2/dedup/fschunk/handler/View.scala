package de.pc2.dedup.fschunk.handler

import scopt.OParser

import scala.collection.mutable.ListBuffer
import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.util.Log

import scala.collection.mutable

/** Handler to view a trace file
  */
class ViewHandler(outputOnlyFingerprint: Boolean)
    extends Reporting
    with FileDataHandler
    with Log {
    val filePartialMap = mutable.Map.empty[String, ListBuffer[Chunk]]
    val startTime: Long = System.currentTimeMillis()

    logger.debug("Start")

    override def report(): Unit = {}

    def handle(fp: FilePart): Unit = {
        logger.debug("View file %s (partial)".format(fp.filename))
        if (!filePartialMap.contains(fp.filename)) {
            filePartialMap += (fp.filename -> new ListBuffer[Chunk]())
        }
        for (chunk <- fp.chunks) {
            filePartialMap(fp.filename).append(chunk)
        }
    }

    def handle(f: File): Unit = {
        logger.debug(
          "View file %s, chunks %s".format(f.filename, f.chunks.size)
        )

        val allFileChunks = if (filePartialMap.contains(f.filename)) {
            val partialChunks = filePartialMap(f.filename)
            filePartialMap -= f.filename
            List.concat(partialChunks.toList, f.chunks)
        } else {
            f.chunks
        }

        if (!outputOnlyFingerprint) {
            val msg = f.label match {
                case Some(l) =>
                    "File %s, size %s, type %s, label %s".format(
                      f.filename,
                      f.fileSize,
                      f.fileType,
                      l
                    )
                case None =>
                    "File %s, size %s, type %s".format(
                      f.filename,
                      f.fileSize,
                      f.fileType
                    )
            }
            println(msg)
        }

        var offset = 0L
        for (chunk <- allFileChunks) {
            val msg = if (outputOnlyFingerprint) {
                "%s".format(chunk.fp)
            } else {
                chunk.chunkHash match {
                    case Some(ch) =>
                        "Chunk %s, offset %s, size %s, chunk hash %s".format(
                          chunk.fp,
                          offset,
                          chunk.size,
                          ch.toHexString
                        )
                    case None =>
                        "Chunk %s, offset %s, size %s".format(
                          chunk.fp,
                          offset,
                          chunk.size
                        )
                }
            }
            offset += chunk.size
            println(msg)
        }
    }

    override def quit(): Unit = {
        logger.debug("Exit")
    }
}

case class ViewConfig(
    filenames: Seq[String] = Seq(),
    format: String = "protobuf",
    onlyFingerprint: Boolean = false
)

object View {
    def main(args: Array[String]): Unit = {

        val builder = OParser.builder[ViewConfig]
        val parser = {
            import builder._
            OParser.sequence(
              programName("fs-c view"),
              head("fs-c", "0.4.0"),
              opt[String]("format")
                  .valueName("<trace file format>")
                  .action((x, c) =>
                      if (Format.isFormat(x)) {
                          c.copy(format = x)
                      } else {
                          println("Invalid fsf-c file format")
                          sys.exit(1)
                      }
                  )
                  .text("Trace file format"),
              opt[Boolean]("only-fingerprint")
                  .action((_, c) => c.copy(onlyFingerprint = true))
                  .text("Outputs only chunk fingerprints"),
              arg[Seq[String]]("Input Files")
                  .valueName("<file1>,<file2>,...")
                  .action((x, c) => c.copy(filenames = x))
                  .text("Trace files to be parsed")
            )
        }

        val config: ViewConfig =
            OParser.parse(parser, args, ViewConfig()) match {
                case Some(c) => c
                case _       => sys.exit(1)
            }

        for (filename <- config.filenames) {
            val viewHandler = new ViewHandler(config.onlyFingerprint)
            val reader =
                Format(config.format).createReader(filename, viewHandler)
            reader.parse()
            viewHandler.quit()
        }
    }
}
