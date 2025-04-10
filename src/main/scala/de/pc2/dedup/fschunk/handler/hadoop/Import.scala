package de.pc2.dedup.fschunk.handler.hadoop

import scopt.OParser

import java.io.FileInputStream
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.Writer
import java.net.URI
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.StringBuilder
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.BZip2Codec
import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

import scala.collection.mutable
import scala.collection.mutable.Buffer

class FileManager(
    fs: FileSystem,
    rootPath: Path,
    suffix: String,
    compress: Boolean
) extends Log {
    val globPath = new Path(rootPath, suffix + "*")
    for (fileStatus <- fs.globStatus(globPath)) {
        if (fs.exists(fileStatus.getPath())) {
            logger.warn("Overwritting " + fileStatus.getPath())
            fs.delete(fileStatus.getPath(), true)
        }
    }

    val files: mutable.Buffer[Writer] = mutable.ListBuffer[Writer]()
    val streams = mutable.ListBuffer[OutputStream]()
    val uniqueId = new AtomicInteger(0);
    val threadLocalFile =
        new ThreadLocal[Writer]() {
            override def initialValue(): Writer = {
                val id = uniqueId.getAndIncrement()
                val filePath = if (compress) {
                    new Path(rootPath, suffix + id + ".bz2")
                } else {
                    new Path(rootPath, suffix + id)
                }
                val stream = if (compress) {
                    createCompressedStream(fs, filePath)
                } else {
                    fs.create(filePath)
                }
                streams += stream
                val writer = new OutputStreamWriter(stream)
                files += writer

                writer
            }
        }

    def createCompressedStream(fs: FileSystem, filepath: Path): OutputStream = {
        val codec = new BZip2Codec()
        val rawStream = fs.create(filepath)
        codec.createOutputStream(rawStream)
    }

    def localOutputWriter: Writer = {
        return threadLocalFile.get()
    }

    def quit(): Unit = {
        for (file <- files) {
            file.flush()
            file.close()
        }
        for (stream <- streams) {
            stream.flush()
            stream.close()
        }
    }
}

/** Handler to import a file into hadoop
  */
class ImportHandler(
    filesystemName: String,
    filename: String,
    threadCount: Int,
    compress: Boolean,
    withFingerprint: Boolean
) extends Reporting
    with FileDataHandler
    with Log {
    val conf = new Configuration()
    val fs = FileSystem.get(new URI(filesystemName), conf)
    val rootPath = new Path(filesystemName, filename)
    val fileDataManager = new FileManager(fs, rootPath, "files", compress)

    val fileFingerprintDataManager = if (withFingerprint) {
        new FileManager(fs, rootPath, "file-fingerprint", compress)
    } else {
        null
    }
    val chunkDataManager = new FileManager(fs, rootPath, "chunks", compress)

    var totalFileSize = new AtomicLong()
    var totalChunkSize = new AtomicLong()
    var totalFileCount = new AtomicLong()
    var totalChunkCount = new AtomicLong()
    val startTime: Long = System.currentTimeMillis()

    /* Only used with withFingerprint
     */
    val openFileMap = mutable.Map.empty[String, MessageDigest]

    logger.debug("Start")
    logger.info("Write path %s".format(rootPath))

    class ImportThreadPoolExecutor()
        extends ThreadPoolExecutor(
          1,
          threadCount,
          30,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue[Runnable](32),
          new ThreadPoolExecutor.CallerRunsPolicy()
        ) {}
    val executor: ImportThreadPoolExecutor = if (threadCount > 0) {
        new ImportThreadPoolExecutor()
    } else {
        null
    }

    class FilePartRunnable(fp: FilePart) extends Runnable {
        override def run(): Unit = {
            val base64 = new Base64()

            logger.debug("Write file %s (partial)".format(fp.filename))

            for (chunk <- fp.chunks) {
                if (withFingerprint) {
                    openFileMap synchronized {
                        getFileDigestBuilder(fp.filename).update(
                          chunk.fp.digest
                        )
                    }
                }
                val chunkWriter = chunkDataManager.localOutputWriter
                chunkWriter.write(getChunkLine(fp.filename, chunk))

                totalChunkSize.addAndGet(chunk.size)
            }
            totalChunkCount.addAndGet(fp.chunks.size)
        }
    }

    def getChunkLine(filename: String, chunk: Chunk): String = {
        val base64 = new Base64()
        val fp: String = base64.encodeToString(chunk.fp.digest)
        val sb = new StringBuilder()
        sb.append(filename)
        sb.append('\t')
        sb.append(fp)
        sb.append('\t')
        sb.append(chunk.size)
        sb.append('\n')
        return sb.toString()
    }

    def getFileLine(f: File): String = {
        val l = f.label match {
            case Some(s) => s
            case None    => "-"
        }

        val sb = new StringBuilder()
        sb.append(f.filename)
        sb.append('\t')
        sb.append(f.fileSize)
        sb.append('\t')
        sb.append(f.fileType)
        sb.append('\t')
        sb.append(l)
        sb.append('\n')
        return sb.toString()
    }
    class FileRunnable(f: File) extends Runnable {
        override def run(): Unit = {
            logger.debug(
              "Write file %s, chunks %s".format(f.filename, f.chunks.size)
            )

            val fileWriter = fileDataManager.localOutputWriter
            fileWriter.write(getFileLine(f))

            for (chunk <- f.chunks) {
                if (withFingerprint) {
                    openFileMap synchronized {
                        getFileDigestBuilder(f.filename).update(chunk.fp.digest)
                    }
                }
                val chunkWriter = chunkDataManager.localOutputWriter
                chunkWriter.write(getChunkLine(f.filename, chunk))
                totalChunkSize.addAndGet(chunk.size)
            }

            if (withFingerprint) {
                openFileMap synchronized {
                    val base64 = new Base64()
                    val fileFingerprint: String = base64.encodeToString(
                      getFileDigestBuilder(f.filename).digest()
                    )
                    val fileFingerprintLine =
                        "%s\t%s%n".format(f.filename, fileFingerprint)

                    val fileFingerpintWriter =
                        fileFingerprintDataManager.localOutputWriter
                    fileFingerpintWriter.write(fileFingerprintLine)
                    openFileMap -= f.filename
                }
            }
            totalFileSize.addAndGet(f.fileSize)
            totalFileCount.addAndGet(1)
            totalChunkCount.addAndGet(f.chunks.size)
        }
    }

    override def report(): Unit = {
        val secs = ((System.currentTimeMillis() - startTime) / 1000)
        if (secs > 0) {
            val mbs = totalFileSize.get() / secs
            val fps = totalFileCount.get() / secs
            logger.info(
              "File Count: %d (%d f/s), File Size %s (%s/s), Chunk Size %s, Chunk Count: %d"
                  .format(
                    totalFileCount.get(),
                    fps,
                    StorageUnit(totalFileSize.get()),
                    StorageUnit(mbs),
                    StorageUnit(totalChunkSize.get()),
                    totalChunkCount.get()
                  )
            )
        }
    }

    def handle(fp: FilePart): Unit = {
        val r = new FilePartRunnable(fp)
        if (executor != null) {
            executor.execute(r)
        } else {
            r.run()
        }
    }

    def getFileDigestBuilder(filename: String): MessageDigest = {
        if (openFileMap.contains(filename)) {
            openFileMap(filename)
        } else {
            val md = MessageDigest.getInstance("MD5")
            openFileMap += (filename -> md)
            md
        }
    }

    def handle(f: File): Unit = {
        val r = new FileRunnable(f)
        if (executor != null) {
            executor.execute(r)
        } else {
            r.run()
        }
    }

    override def quit(): Unit = {
        if (executor != null) {
            executor.shutdown()
            executor.awaitTermination(600L, TimeUnit.SECONDS)
        }

        fileDataManager.quit()
        if (fileFingerprintDataManager != null) {
            fileFingerprintDataManager.quit()
        }
        chunkDataManager.quit()
        report()
        logger.debug("Exit")

    }
}

case class ImportConfig(
    filenames: Seq[String] = Seq(),
    report: Int = 60,
    output: Option[String] = None,
    fileFingerprint: Boolean = false,
    compress: Boolean = false,
    threads: Int = 1,
    format: String = "protobuf"
)

object Import {
    def main(args: Array[String]): Unit = {

        val builder = OParser.builder[ImportConfig]
        val parser = {
            import builder._
            OParser.sequence(
              programName("fs-c import"),
              head("fs-c", "0.4.0"),
              opt[Int]('r', "report")
                  .valueName("<seconds>")
                  .action((x, c) => c.copy(report = x))
                  .text("Interval between progress reports (default=60)"),
              opt[String]('o', "output")
                  .valueName("<dir>")
                  .action((x, c) => c.copy(output = Some(x)))
                  .text("HDFS directory for output")
                  .required(),
              opt[Boolean]("file-fingerprint")
                  .action((_, c) => c.copy(fileFingerprint = true))
                  .text("Import with file fingerprints"),
              opt[Boolean]("compress")
                  .action((_, c) => c.copy(compress = true))
                  .text("Compress the output"),
              opt[Int]('j', "jobs")
                  .valueName("<jobs>")
                  .action((x, c) => c.copy(threads = x))
                  .text("Number of concurrent jobs"),
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
              arg[Seq[String]]("Input Filenames")
                  .valueName("<file1>,<file2>,...")
                  .action((x, c) => c.copy(filenames = x))
                  .text("Input trace files to parse")
            )
        }

        val config: ImportConfig =
            OParser.parse(parser, args, ImportConfig()) match {
                case Some(c) => c
                case _       => sys.exit(1)
            }

        for (filename <- config.filenames) {
            val handler = new ImportHandler(
              config.output.get,
              config.output.get,
              config.threads,
              config.compress,
              config.fileFingerprint
            )
            val stream = if (filename.startsWith("hdfs://")) {
                val conf = new Configuration()
                val fs = FileSystem.get(new URI(filename), conf)
                val path = new Path(filename)
                fs.open(path)
            } else { new FileInputStream(filename) }

            val reader = Format(config.format).createReader(stream, handler)
            val reporter = new Reporter(handler, config.report).start()
            reader.parse()

            reporter.quit()
            handler.quit()
        }
    }
}
