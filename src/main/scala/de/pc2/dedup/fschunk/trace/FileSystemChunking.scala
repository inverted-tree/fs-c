package de.pc2.dedup.fschunk.trace

import java.io.File
import java.io.IOException

import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.util.Log

/** Main tracing class
  */
class FileSystemChunking(
    fileListings: FileListingProvider,
    chunkers: Seq[(Chunker, List[FileDataHandler])],
    threads: Int,
    useDefaultIgnores: Boolean,
    followSymlinks: Boolean,
    useRelativePaths: Boolean,
    useJavaDirectoryListing: Boolean,
    clustered: Boolean,
    progressHandler: (de.pc2.dedup.chunker.File) => Unit
) extends Log
    with Reporting {

    /** Dispatching object
      */
    val dispatcher: FileDispatcher & Log =
        if (clustered)
            new DistributedFileDispatcher(
              threads,
              chunkers,
              useDefaultIgnores,
              followSymlinks,
              useRelativePaths,
              useJavaDirectoryListing,
              progressHandler
            )
        else
            new ThreadPoolFileDispatcher(
              threads,
              chunkers,
              useDefaultIgnores,
              followSymlinks,
              useRelativePaths,
              useJavaDirectoryListing,
              progressHandler
            )

    def report(): Unit = {
        dispatcher.report()
        for ((_, handlers) <- chunkers) {
            handlers.foreach(h => h.report())
        }
    }

    logger.debug("Start chunking")

    // Append all files from listing to directory processor
    if (dispatcher.isLeader) {
        for (listing <- fileListings) {
            val file = getFile(listing.filename)
            dispatcher.dispatch(
              file,
              file.getCanonicalPath,
              file.isDirectory,
              listing.source,
              listing.label
            )
        }
    }

    def start(): Unit = {
        dispatcher.waitUntilFinished()
        logger.info("Tracing finished")

        for ((_, handlers) <- chunkers) {
            handlers.foreach(h => h.quit())
        }
    }

    def quit(): Unit = dispatcher.quit()

    private def getFile(filename: String): File = {
        if (filename.equals(".")) {
            try {
                new File(filename).getCanonicalFile
            } catch {
                case _: IOException => new File(filename)
            }
        } else {
            new File(filename)
        }
    }
}
