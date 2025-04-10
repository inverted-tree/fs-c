package de.pc2.dedup.fschunk.trace

import com.hazelcast.core.{Hazelcast, HazelcastInstance}
import scopt.OParser

import java.io.File
import de.pc2.dedup.chunker.fixed.FixedChunker
import de.pc2.dedup.chunker.rabin.RabinChunker
import de.pc2.dedup.chunker.{Chunker, ChunkerFactory, DigestFactory}
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.direct.ChunkIndex
import de.pc2.dedup.fschunk.handler.direct.InMemoryChunkHandler
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.GCReporting
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.SystemExitException

case class TraceConfig(
    filenames: Seq[File] = Seq(),
    chunkerNames: Seq[String] = Seq(),
    customHandler: Option[String] = None,
    digestLength: Int = 20,
    digestType: String = "SHA-1",
    distributed: Boolean = false,
    followSymlinks: Boolean = false,
    label: Option[String] = None,
    listing: Boolean = false,
    logChunkHashes: Boolean = false,
    memoryReporting: Boolean = false,
    output: Option[File] = None,
    privacyMode: PrivacyMode = PrivacyMode.None,
    progressFile: Option[File] = None,
    silent: Boolean = false,
    salt: Option[String] = None,
    threads: Int = 1,
    useDefaultIgnores: Boolean = true,
    useJavaDirectoryListing: Boolean = false,
    reportInterval: Int = 60,
    relativePaths: Boolean = false
)

object Main extends Log {
    def main(args: Array[String]): Unit = {
        val builder = OParser.builder[TraceConfig]
        val parser = {
            import builder._
            OParser.sequence(
              programName("fs-c"),
              head("fs-c", "0.4.0"),
              opt[Seq[String]]('c', "chunker")
                  .valueName("<chunker1>,<chunker2>,...")
                  .action((x, c) => c.copy(chunkerNames = x))
                  .text("Explicitly set the chunker(s)"),
              opt[Int]("digest-length")
                  .valueName("<Int>")
                  .action((x, c) => c.copy(digestLength = x))
                  .text("Length of the digest / fingerprint (default = 20)"),
              opt[String]("digest-type")
                  .valueName("SHA-1/...")
                  .action((x, c) => c.copy(digestType = x))
                  .text("Type of fingerprint to be used"),
              opt[Unit]('d', "distributed")
                  .action((_, c) => c.copy(distributed = true))
                  .text("Run the program in cluster mode"),
              opt[Unit]("follow-symlinks")
                  .action((_, c) => c.copy(followSymlinks = true))
                  .text("Follow symlinks in input files"),
              opt[Option[String]]("handler")
                  .valueName("<handler>")
                  .action((x, c) => c.copy(customHandler = x))
                  .text("Fully qualified class name of a custom chunk handler"),
              opt[Boolean]("java-dir-listing")
                  .action((_, c) => c.copy(useJavaDirectoryListing = true))
                  .text(
                    "Uses Javas builtin directory listing instead of the default method"
                  ),
              opt[Int]('j', "jobs")
                  .action((x, c) => c.copy(threads = x))
                  .text("Number of concurrent jobs to run"),
              opt[Option[String]]("label")
                  .valueName("<label>")
                  .action((x, c) => c.copy(label = x))
                  .text("Use the file label"),
              opt[Unit]('l', "listing")
                  .action((_, c) => c.copy(listing = true))
                  .text("Input file is a listing of files to trace"),
              opt[Boolean]("log-hashes")
                  .action((_, c) => c.copy(logChunkHashes = true))
                  .text("Log chunker hashes in trace file"),
              opt[Boolean]("memory-usage")
                  .action((_, c) => c.copy(memoryReporting = true))
                  .text("Report memory usage"),
              opt[Boolean]("no-default-ignores")
                  .action((_, c) => c.copy(useDefaultIgnores = false))
                  .text("Don't use the default ignore list"),
              opt[Option[File]]('o', "output")
                  .valueName("<file>")
                  .action((x, c) => c.copy(output = x))
                  .text("Print output to a file"),
              opt[String]("privacy-mode")
                  .valueName("<mode>")
                  .action((x, c) =>
                      x match {
                          case "none" => c.copy(privacyMode = PrivacyMode.None)
                          case "reversible" =>
                              c.copy(privacyMode = PrivacyMode.Reversible)
                          case "hash" => c.copy(privacyMode = PrivacyMode.Hash)
                          case "dir-hash" =>
                              c.copy(privacyMode = PrivacyMode.DirHash)
                          case _ =>
                              println("Invalid option %s".format(x))
                              sys.exit(1)
                      }
                  )
                  .text(
                    "Set the privacy mode. Options are: none (=default) | reversible | hash | dir-hash"
                  ),
              opt[Option[File]]("progress-file")
                  .valueName("<file>")
                  .action((x, c) => c.copy(progressFile = x))
                  .text("Track already traced files in the provided file"),
              opt[Int]('r', "report")
                  .action((x, c) => c.copy(reportInterval = x))
                  .text(
                    "Interval between progress reports in seconds (default = 60, 0 = no report)"
                  ),
              opt[Option[String]]("salt")
                  .valueName("<salt>")
                  .action((x, c) => c.copy(salt = x))
                  .text("Salt the fingerprints"),
              opt[Unit]('s', "silent")
                  .action((_, c) => c.copy(silent = true))
                  .text("Reduced output"),
              opt[Boolean]("store-relative-path")
                  .action((_, c) => c.copy(relativePaths = true))
                  .text("Stores only relative paths"),
              arg[Seq[File]]("Input Files")
                  .valueName("<file1>,<file2>,...")
                  .action((x, c) => c.copy(filenames = x))
                  .text("Files to be traced")
            )
        }

        val config: TraceConfig =
            OParser.parse(parser, args, TraceConfig()) match {
                case Some(c) => c
                case _       => sys.exit(1)
            }

        val format = "protobuf"

        def getHandler(chunkerName: String): List[FileDataHandler] = {
            val handler = config.output match {
                case None =>
                    config.customHandler match {
                        case Some(className) =>
                            try {
                                val customHandler = Class
                                    .forName(className)
                                    .getDeclaredConstructor()
                                    .newInstance()
                                    .asInstanceOf[FileDataHandler]
                                customHandler :: Nil
                            } catch {
                                case e: Exception =>
                                    throw new Exception(
                                      "Failed to instanciate the provided chunk handler %s: %s"
                                          .format(className, e)
                                    )
                            }
                        case None =>
                            new InMemoryChunkHandler(
                              config.silent,
                              new ChunkIndex,
                              Some(chunkerName)
                            ) :: Nil
                    }
                case Some(o) =>
                    config.customHandler match {
                        case Some(className) =>
                            throw new Exception(
                              "Cannot use custom chunk handler with output option"
                            )
                        case None => // ok
                    }
                    val outputFile = if (config.distributed) {
                        val hcInstance: HazelcastInstance =
                            Hazelcast.newHazelcastInstance()
                        val memberID =
                            hcInstance.getCluster.getLocalMember.getSocketAddress.getHostName
                        "%s-%s-%s".format(o, config.chunkerNames, memberID)
                    } else {
                        "%s-%s".format(o, config.chunkerNames)
                    }
                    Format(format).createWriter(
                      outputFile,
                      config.privacyMode
                    ) :: Nil
            }
            handler
        }
        val chunkers = for {
            chunkerName <- config.chunkerNames
        } yield (
          ChunkerFactory.createChunker(
            chunkerName,
            config.logChunkHashes,
            config.digestType,
            config.digestLength,
            config.salt
          ),
          getHandler(chunkerName)
        )

        val fileListing: FileListingProvider = if (config.listing) {
            FileListingProvider.fromListingFile(config.filenames, config.label)
        } else {
            FileListingProvider.fromDirectFile(config.filenames, config.label)
        }

        val progressHandler = config.progressFile match {
            case Some(filename) =>
                val outputFilename = if (config.distributed) {
                    val hcInstance: HazelcastInstance =
                        Hazelcast.newHazelcastInstance()
                    val memberID = Hazelcast
                        .newHazelcastInstance()
                        .getCluster
                        .getLocalMember
                        .getSocketAddress
                        .getHostName
                    "%s-%s".format(filename, memberID)
                } else {
                    filename
                }
                val fileProgressHandler = new FileProgressHandler(
                  filename.toString
                )
                fileProgressHandler.progress
            case None =>
                def dummyProgressHandler(
                    f: de.pc2.dedup.chunker.File
                ): Unit = {}
                dummyProgressHandler
        }

        val chunkingPipeline = new FileSystemChunking(
          fileListings = fileListing,
          chunkers = chunkers,
          threads = config.threads,
          useDefaultIgnores = config.useDefaultIgnores,
          followSymlinks = config.followSymlinks,
          useRelativePaths = config.relativePaths,
          useJavaDirectoryListing = config.useJavaDirectoryListing,
          clustered = config.distributed,
          progressHandler = progressHandler
        )
        val reporter =
            new Reporter(chunkingPipeline, config.reportInterval).start()
        val memoryReporter = if (config.memoryReporting) {
            Some(new Reporter(new GCReporting(), config.reportInterval).start())
        } else { None }

        chunkingPipeline.start()
        reporter.quit()
        memoryReporter match {
            case Some(r) => r.quit()
            case None    => // pass
        }
        chunkingPipeline.report()
        chunkingPipeline.quit()
    }
}
