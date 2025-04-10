package de.pc2.dedup.fschunk.handler.gp

import scopt.OParser

import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.commons.codec.binary.Base64
import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.direct.StandardReportingHandler
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.parse.Parser
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.util.Log
import de.pc2.dedup.fschunk.format.Format

/** Handler to import a file into a Greenplum database
  */
class GPImportHandler(output: String) extends FileDataHandler with Log {

    val fileOutputStream = new BufferedWriter(
      new FileWriter("%s-gp-file.csv".format(output))
    )
    val chunkOutputStream = new BufferedWriter(
      new FileWriter("%s-gp-chunk.csv".format(output))
    )
    val base64 = new Base64()

    private def handleChunk(filename: String, chunk: Chunk): Unit = {
        val encodedDigest = base64.encodeToString(chunk.fp.digest)
        val line =
            "\"%s\"|\"%s\"|%s\n".format(filename, encodedDigest, chunk.size)
        chunkOutputStream.write(line)

    }

    def handle(fp: FilePart): Unit = {
        for (chunk <- fp.chunks) {
            handleChunk(fp.filename, chunk)
        }
    }

    def handle(f: File): Unit = {
        for (chunk <- f.chunks) {
            handleChunk(f.filename, chunk)
        }

        val label = f.label match {
            case None    => ""
            case Some(l) => l
        }
        val line = "\"%s\"|%s|\"%s\"|\"%s\"\n".format(
          f.filename,
          f.fileSize,
          f.fileType,
          label
        )
        fileOutputStream.write(line)
    }

    override def quit(): Unit = {
        fileOutputStream.close()
        chunkOutputStream.close()
    }
}

case class GPImportConfig(
    format: String = "protobuf",
    outputFile: Option[String] = None,
    report: Integer = 60,
    filenames: Seq[String] = Seq()
)

object GPImport {
    def main(args: Array[String]): Unit = {
        import builder._

        val builder = OParser.builder[GPImportConfig]

        val parser = {
            import builder._
            OParser.sequence(
              programName("fs-c gpimport"),
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
              opt[String]('o', "output")
                  .valueName("<file>")
                  .action((x, c) => c.copy(outputFile = Some(x)))
                  .text("Output file")
                  .required(),
              opt[Int]('r', "report")
                  .valueName("<seconds>")
                  .action((x, c) => c.copy(report = x))
                  .text("Interval between progress reports (default = 60)"),
              arg[Seq[String]]("Input Files")
                  .valueName("<file1>,<file2>,...")
                  .action((x, c) => c.copy(filenames = x))
                  .text("Trace files to be parsed")
            )
        }

        val config: GPImportConfig =
            OParser.parse(parser, args, GPImportConfig()) match {
                case Some(c) => c
                case _       => sys.exit(1)
            }

        val importHandler = new GPImportHandler(config.outputFile.get)
        val handlerList = List(importHandler, new StandardReportingHandler())

        for (filename <- config.filenames) {
            val p = new Parser(filename, config.format, handlerList)
            val reporter = new Reporter(p, config.report).start()

            p.parse()
            reporter.quit()
        }

        for (handler <- handlerList) {
            handler.quit()
        }
    }
}
