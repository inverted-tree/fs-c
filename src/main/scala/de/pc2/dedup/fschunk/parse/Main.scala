package de.pc2.dedup.fschunk.parse

import scopt.OParser

import scala.collection.mutable.ListBuffer
import de.pc2.dedup.fschunk.handler.direct.ChunkIndex
import de.pc2.dedup.fschunk.handler.direct.FullFileRedundancyHandler
import de.pc2.dedup.fschunk.handler.direct.ChunkSizeDistributionHandler
import de.pc2.dedup.fschunk.handler.direct.DeduplicationHandler
import de.pc2.dedup.fschunk.handler.direct.FileDetailsHandler
import de.pc2.dedup.fschunk.handler.direct.FileStatisticsHandler
import de.pc2.dedup.fschunk.handler.direct.InMemoryChunkHandler
import de.pc2.dedup.fschunk.handler.direct.InternalRedundancyHandler
import de.pc2.dedup.fschunk.handler.direct.StandardReportingHandler
import de.pc2.dedup.fschunk.handler.direct.TemporalRedundancyHandler
import de.pc2.dedup.fschunk.handler.direct.ZeroChunkDeduplicationHandler
import de.pc2.dedup.fschunk.handler.harnik.HarnikEstimationSample
import de.pc2.dedup.fschunk.handler.harnik.HarnikEstimationSamplingHandler
import de.pc2.dedup.fschunk.handler.harnik.HarnikEstimationScanHandler
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.util.SystemExitException
import de.pc2.dedup.fschunk.GCReporting
import de.pc2.dedup.fschunk.format.Format
import java.io.File
import de.pc2.dedup.util.Log

/** Main object for the parser. The parser is used to replay trace of chunking
  * runs
  */
object Main extends Log {

    def getCustomHandler(handlerName: String): FileDataHandler = {
        try {
            Class.forName(handlerName).newInstance.asInstanceOf[FileDataHandler]
        } catch {
            case ioe: ClassNotFoundException =>
                Class
                    .forName(
                      "de.pc2.dedup.fschunk.handler.direct." + handlerName
                    )
                    .newInstance
                    .asInstanceOf[FileDataHandler]
        }
    }

    case class ParseConfig(
        filenames: Seq[String] = Seq(),
        parseType: Seq[String] = Seq(),
        output: Option[String] = None,
        format: String = "protobuf",
        report: Int = 60,
        sampleSize: Option[Int] = None,
        memoryUsage: Boolean = false
    )

    /** @param args
      *   the command line arguments
      */
    def main(args: Array[String]): Unit = {
        val builder = OParser.builder[ParseConfig]
        val parser = {
            import builder._
            OParser.sequence(
              programName("fs-c"),
              head("fs-c", "0.4.0"),
              opt[Seq[String]]('t', "type")
                  .valueName("<type>")
                  .action((x, c) => c.copy(parseType = x))
                  .text(
                    "Set the handler type [simple | ir | tr | harniks-tr | full-file | file-stats | file-details | chunk-size-stats | zero-chunk | custom]"
                  ),
              opt[Option[String]]('o', "output")
                  .action((x, c) => c.copy(output = x))
                  .text("Output file"),
              opt[String]("format")
                  .action((x, c) =>
                      if (Format.isFormat(x)) {
                          c.copy(format = x)
                      } else {
                          c.copy(format = "protobuf")
                      }
                  )
                  .text("Trace file format"),
              opt[Int]('r', "report")
                  .valueName("<seconds>")
                  .action((x, c) => c.copy(report = x))
                  .text("Interval between progress reports"),
              opt[Option[Int]]("harnik-sample-size")
                  .action((x, c) => c.copy(sampleSize = x))
                  .text("Number of samples for the harnik type"),
              opt[Boolean]("memory-usage")
                  .action((_, c) => c.copy(memoryUsage = true))
                  .text("Report memory usage"),
              arg[Seq[String]]("Input Files")
                  .valueName("<file1>,<file2>,...")
                  .action((x, c) => c.copy(filenames = x))
                  .text("Files to be parsed")
            )
        }

        val config: ParseConfig =
            OParser.parse(parser, args, ParseConfig()) match {
                case Some(c) => c
                case _       => sys.exit(1)
            }

        val handlers = if (config.parseType.isEmpty) {
            for {
                t <- config.parseType
            } yield t
        } else {
            Seq("simple")
        }

        val memoryUsageReporter = if (config.memoryUsage) {
            Some(new Reporter(new GCReporting(), config.report).start())
        } else {
            None
        }

        if (
          !handlers
              .contains("tr") && !handlers.contains("harniks-tr")
        ) {
            val handlerList: List[FileDataHandler] = gatherHandlerList(
              handlers,
              config.sampleSize,
              config.output
            )
            executeParsing(
              handlerList,
              config.format,
              Some(config.report),
              config.filenames
            )

            if (handlers.contains("harniks")) {
                // we need a second run
                val estimationSample = getHarnikEstimationHandler(
                  handlerList
                )

                val handlerList2 = List(
                  new HarnikEstimationScanHandler(
                    estimationSample,
                    config.output
                  ),
                  new StandardReportingHandler()
                )
                executeParsing(
                  handlerList2,
                  config.format,
                  Some(config.report),
                  config.filenames
                )

                handlerList.foreach(_.quit())
                handlerList2.foreach(_.quit())
            } else {
                handlerList.foreach(_.quit())
            }
        } else {
            if (handlers.size > 1) {
                println(
                  "Illegal type configuration: tr cannot only be used alone"
                )
            }
            val handlerType = handlers.head
            handlerType match {
                case "tr" =>
                    if (config.filenames.size < 2) {
                        println(
                          "tr type has to be started with two at least two files"
                        )
                    }
                    runTemporalHandlers(
                      config.format,
                      Some(config.report),
                      config.filenames
                    )
                case "harniks-tr" =>
                    if (config.filenames.size < 2) {
                        println(
                          "harniks-tr type has to be started with at least two files"
                        )
                    }

                    config.output match {
                        case Some(s) =>
                            println(
                              "harniks-tr cannot be used with --output option"
                            )
                        case None =>
                            runTemporalHarnikHandlers(
                              config.format,
                              Some(config.report),
                              config.sampleSize,
                              config.filenames
                            )
                    }
            }
        }
        memoryUsageReporter match {
            case Some(r) => r.quit()
            case None    => // pass
        }
    }

    private def runTemporalHandlers(
        format: String,
        reportInterval: Option[Int],
        filenames: Seq[String]
    ): Unit = {
        val filenameList = (filenames, filenames.tail).zipped.toList

        var handler = new DeduplicationHandler(new ChunkIndex())
        executeParsing(
          List(handler, new StandardReportingHandler()),
          format,
          reportInterval,
          List(filenames(0))
        )
        handler.quit()

        for ((filename1, filename2) <- filenameList) {

            val temporalHandler = new TemporalRedundancyHandler(None, handler.d)

            // handler for the next file
            handler = new DeduplicationHandler(new ChunkIndex())
            executeParsing(
              List(temporalHandler, handler, new StandardReportingHandler()),
              format,
              reportInterval,
              List(filename2)
            )

            println("%s -> %s".format(filename1, filename2))
            temporalHandler.quit()

        }
    }

    private def runTemporalHarnikHandlers(
        format: String,
        reportInterval: Option[Int],
        harnikSampleCount: Option[Int],
        filenames: Seq[String]
    ): Unit = {
        val filenameList = (filenames, filenames.tail).zipped.toList

        var sampleHandler =
            new HarnikEstimationSamplingHandler(harnikSampleCount, None)
        var singleSampleHandler =
            new HarnikEstimationSamplingHandler(harnikSampleCount, None)

        executeParsing(
          List(
            sampleHandler,
            singleSampleHandler,
            new StandardReportingHandler()
          ),
          format,
          reportInterval,
          List(filenames(0))
        )

        // we overlap the different sample/scan runs to decrease the number of total passes
        // over the data
        for ((filename1, filename2) <- filenameList) {
            // filename1 run has already been performed
            executeParsing(
              List(sampleHandler, new StandardReportingHandler()),
              format,
              reportInterval,
              List(filename2)
            )
            sampleHandler.quit()
            singleSampleHandler.quit()

            val sample = sampleHandler.estimationSample
            val singleSample = singleSampleHandler.estimationSample

            val scanHandler = new HarnikEstimationScanHandler(sample, None)
            val singleScanHandler =
                new HarnikEstimationScanHandler(singleSample, None)

            executeParsing(
              List(
                scanHandler,
                singleScanHandler,
                new StandardReportingHandler()
              ),
              format,
              reportInterval,
              List(filename1)
            )

            // overlap with the next sample run
            sampleHandler =
                new HarnikEstimationSamplingHandler(harnikSampleCount, None)
            singleSampleHandler =
                new HarnikEstimationSamplingHandler(harnikSampleCount, None)

            executeParsing(
              List(
                scanHandler,
                sampleHandler,
                singleSampleHandler,
                new StandardReportingHandler()
              ),
              format,
              reportInterval,
              List(filename2)
            )

            println("%s -> %s".format(filename1, filename2))
            HarnikEstimationScanHandler.outputTemporalScanResult(
              sample,
              scanHandler.estimator,
              singleScanHandler.estimator
            )
            println()
        }
        HarnikEstimationScanHandler.outputNaNWarning()
    }

    private def getHarnikEstimationHandler(
        handlerList: Seq[FileDataHandler]
    ): HarnikEstimationSample = {
        for (handler <- handlerList) {
            if (handler.isInstanceOf[HarnikEstimationSamplingHandler]) {
                val samplingHandler =
                    handler.asInstanceOf[HarnikEstimationSamplingHandler]
                return samplingHandler.estimationSample
            }
        }
        throw new Exception("Failed to find estimation sample")
    }

    private def gatherHandlerList(
        handlerTypeList: Seq[String],
        optionHarnikSampleCount: Option[Int],
        output: Option[String]
    ): List[FileDataHandler] = {
        val handlerList = new ListBuffer[FileDataHandler]()
        handlerList += new StandardReportingHandler()
        for (handlerType <- handlerTypeList) {
            handlerType match {
                case "simple" =>
                    output match {
                        case None =>
                        case _ =>
                            throw new Exception(
                              "Output parameter is not supported by simple handler type"
                            )
                    }
                    handlerList += new InMemoryChunkHandler(
                      false,
                      new ChunkIndex(),
                      None
                    )

                case "ir" =>
                    handlerList += new InternalRedundancyHandler(
                      output,
                      new ChunkIndex()
                    )
                case "file-stats" =>
                    handlerList += new FileStatisticsHandler()
                case "file-details" =>
                    handlerList += new FileDetailsHandler(output)
                case "chunk-size-stats" =>
                    handlerList += new ChunkSizeDistributionHandler()
                case "full-file" =>
                    handlerList += new FullFileRedundancyHandler()
                case "zero-chunk" =>
                    handlerList += new ZeroChunkDeduplicationHandler()
                case "harniks" =>
                    // phase 1
                    handlerList += new HarnikEstimationSamplingHandler(
                      optionHarnikSampleCount,
                      output
                    )
                case "tr" =>
                    throw new Exception("tr needs special treatment")
                case "tr-harniks" =>
                    throw new Exception("harniks-tr need special treatment")
                case customName =>
                    handlerList += getCustomHandler(customName)
            }
        }
        handlerList.toList
    }

    private def executeParsing(
        handlerList: List[FileDataHandler],
        format: String,
        reportInterval: Option[Int],
        filenames: Seq[String]
    ): Unit = {
        for (filename <- filenames) {
            val p = new Parser(filename, format, handlerList)
            val reporter = new Reporter(p, reportInterval.get).start()
            p.parse()
            reporter.quit()
        }
    }
}
