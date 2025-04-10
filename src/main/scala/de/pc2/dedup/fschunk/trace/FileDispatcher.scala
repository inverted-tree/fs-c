package de.pc2.dedup.fschunk.trace

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit
import de.pc2.dedup.util.BlockThenRunPolicy
import java.util.concurrent.RejectedExecutionHandler

/** file dispatching trait
  */
trait FileDispatcher extends Reporting {
    def dispatch(
        f: File,
        path: String,
        isDir: Boolean,
        source: Option[String],
        label: Option[String]
    ): Unit

    def waitUntilFinished(): Unit

    def isLeader: Boolean = true

    def quit(): Unit = {}
}

/** Dispatches files to a number of file processors that chunk the file contents
  */
class ThreadPoolFileDispatcher(
    threads: Int,
    chunkers: Seq[(Chunker, List[FileDataHandler])],
    useDefaultIgnores: Boolean,
    followSymlinks: Boolean,
    useRelativePaths: Boolean,
    useJavaDirectoryListing: Boolean,
    progressHandler: (de.pc2.dedup.chunker.File) => Unit
) extends FileDispatcher
    with Log {
    var lock: AnyRef = new Object()
    var finished: Boolean = false
    val startTime: Long = System.currentTimeMillis()

    val activeAllCount = new AtomicLong()
    val activeDirCount = new AtomicLong()
    val activeFileCount = new AtomicLong()

    private def shouldShutdown(): Boolean = activeAllCount.get() == 0

    private def getRejectionPolicy: RejectedExecutionHandler = {
        if (threads == 1)
            new BlockThenRunPolicy()
        else
            new ThreadPoolExecutor.CallerRunsPolicy()
    }

    class DirectoryDispatcherThreadPoolExecutor(
        dispatcher: ThreadPoolFileDispatcher
    ) extends ThreadPoolExecutor(
          2,
          2,
          30,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue[Runnable](1024),
          getRejectionPolicy
        ) {
        logger.debug(
          "Created directory thread pool with at most %d threads".format(2)
        )
        override def afterExecute(r: Runnable, t: Throwable): Unit = {
            if (shouldShutdown()) {
                dispatcher.executorFinished()
            }
        }
    }

    class DirectoryParentRunnable(runnable: Runnable) extends Runnable {
        override def run(): Unit = {
            try {
                runnable.run()
            } finally {
                activeDirCount.decrementAndGet()
                activeAllCount.decrementAndGet()
            }
        }
    }

    class FileParentRunnable(runnable: Runnable) extends Runnable {
        override def run(): Unit = {
            try {
                runnable.run()
            } finally {
                activeFileCount.decrementAndGet()
                activeAllCount.decrementAndGet()
            }
        }
    }

    class FileDispatcherThreadPoolExecutor(dispatcher: ThreadPoolFileDispatcher)
        extends ThreadPoolExecutor(
          threads,
          threads,
          30,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue[Runnable](threads * 1024),
          getRejectionPolicy
        ) {
        logger.debug(
          "Created file 'threadpool' with at most %d threads".format(threads)
        )
        override def afterExecute(r: Runnable, t: Throwable): Unit = {
            if (shouldShutdown()) {
                dispatcher.executorFinished()
            }
        }
    }

    FileProcessor.init(chunkers, progressHandler, useRelativePaths)
    DirectoryProcessor.init(this, useJavaDirectoryListing)
    val fileExecutor = new FileDispatcherThreadPoolExecutor(this)
    val dirExecutor = new DirectoryDispatcherThreadPoolExecutor(this)

    def dispatch(
        file: File,
        path: String,
        isDir: Boolean,
        source: Option[String],
        label: Option[String]
    ): Unit = {
        val activeCount = activeAllCount.incrementAndGet()
        if (isDir) {
            activeDirCount.incrementAndGet()
            dirExecutor.execute(
              new DirectoryParentRunnable(
                new DirectoryProcessor(
                  file,
                  source,
                  label,
                  useDefaultIgnores,
                  followSymlinks
                )
              )
            )
        } else {
            activeFileCount.incrementAndGet()
            fileExecutor.execute(
              new FileParentRunnable(
                new FileProcessor(file, path, source, label)
              )
            )
        }
    }

    def waitUntilFinished(): Unit = {
        lock.synchronized {
            while (!finished) {
                lock.wait()
            }
        }
    }

    private def executorFinished(): Unit = {
        logger.info("Dispatching finished")

        dirExecutor.shutdown()
        fileExecutor.shutdown()

        lock.synchronized {
            finished = true
            lock.notifyAll()
        }
    }

    def report(): Unit = {
        val secs = ((System.currentTimeMillis() - startTime) / 1000)
        if (secs > 0) {
            val mbs = FileProcessor.totalRead.get() / secs
            logger.info(
              "Total Files: %d, Data: %s (%s/s), Active Files: %d, Scheduled Files: %d, File Pool Size: %d, Total Dirs: %d, Active Dirs: %d, Scheduled Dirs: %d, Dir Pool Size: %d, Skipped Dirs: %d"
                  .format(
                    FileProcessor.totalCount.get(),
                    StorageUnit(FileProcessor.totalRead.get()),
                    StorageUnit(mbs),
                    FileProcessor.activeCount.get(),
                    activeFileCount.get(),
                    fileExecutor.getPoolSize,
                    DirectoryProcessor.totalCount.get(),
                    DirectoryProcessor.activeCount.get(),
                    activeDirCount.get(),
                    dirExecutor.getPoolSize,
                    DirectoryProcessor.skipCount.get()
                  )
            )
        }
    }
}
