package de.pc2.dedup.fschunk.trace

import com.hazelcast.config.{Config, ExecutorConfig}

import java.io.File
import java.lang.Integer as Int
import java.util.concurrent.Future
import com.hazelcast.core.{ExecutionCallback, Hazelcast, HazelcastInstance}
import com.hazelcast.cp.IAtomicLong
import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

/** Cluster version of a file dispatcher
  */
class DistributedFileDispatcher(
    processorNum: Int,
    chunker: Seq[(Chunker, List[FileDataHandler])],
    useDefaultIgnores: Boolean,
    followSymlinks: Boolean,
    useRelativePaths: Boolean,
    useJavaDirectoryListing: Boolean,
    progressHandler: (de.pc2.dedup.chunker.File) => Unit
) extends FileDispatcher
    with Log {
    val startTime: Long = System.currentTimeMillis()
    var lock: AnyRef = new Object()
    var finished: Boolean = false

    private val hcInstance: HazelcastInstance = Hazelcast.newHazelcastInstance()
    val activeAllCount: IAtomicLong =
        hcInstance.getCPSubsystem.getAtomicLong("all count")
    val activeDirCount: IAtomicLong =
        hcInstance.getCPSubsystem.getAtomicLong("dir count")
    val activeFileCount: IAtomicLong =
        hcInstance.getCPSubsystem.getAtomicLong("file count")

    /** id of the instance. Is used for determining a initial leader
      */
    val id: Long =
        hcInstance.getCPSubsystem.getAtomicLong("id").incrementAndGet()

    /** true iff the executor service should be shut down.
      */
    private def shouldShutdown: Boolean = {
        activeAllCount.get() == 0
    }

    /** Adapts the Hazelcast configuration
      */
    private def adaptExecutorConfiguration(): Unit = {
        val config: Config = new Config()
        config.getExecutorConfig("file").setPoolSize(processorNum)
        config.getExecutorConfig("dir").setPoolSize(2)
    }

    adaptExecutorConfiguration()
    private val fileExecutor = hcInstance.getExecutorService("file")
    private val dirExecutor = hcInstance.getExecutorService("dir")

    FileProcessor.init(chunker, progressHandler, useRelativePaths)
    DirectoryProcessor.init(this, useJavaDirectoryListing)

    /** Human readable member id aka hostname
      */
    private val memberId =
        hcInstance.getCluster.getLocalMember.getSocketAddress.getHostName

    logger.info("%s: cluster id %s, leader %s".format(memberId, id, isLeader))

    override def isLeader: Boolean = {
        id == 1
    }

    /** Dispatch the given file to the correct executor service
      */
    def dispatch(
        f: File,
        path: String,
        isDir: Boolean,
        source: Option[String],
        label: Option[String]
    ): Unit = {
        logger.debug("Dispatch %s".format(f))

        activeAllCount.incrementAndGet()
        val runnable = if (isDir) {
            activeDirCount.incrementAndGet()
            new DirectoryProcessor(
              f,
              source,
              label,
              useDefaultIgnores,
              followSymlinks
            )
        } else {
            activeFileCount.incrementAndGet()
            new FileProcessor(f, path, source, label)
        }
        // the task type id is a) necessary for the executor callback and b) is used to determine the
        // correct operations in the executor callback
        val taskTypeId: Int = if (isDir) {
            1
        } else {
            2
        }

        /////////////

        val future: ExecutionCallback[Int] = new ExecutionCallback[Int] {

            override def onResponse(response: Int): Unit = {
                logger.debug("Task finished")

                val taskTypeId = response
                if (taskTypeId == 1) {
                    activeDirCount.decrementAndGet()
                } else {
                    activeFileCount.decrementAndGet()
                }
                activeAllCount.decrementAndGet()
                if (shouldShutdown) {
                    executorFinished()
                }
            }

            override def onFailure(t: Throwable): Unit = {
                logger.error("Task failed", t)
            }
        }
        ///////////

        // create a distributed task instance
//        val task = new DistributedTask[Int](runnable, taskTypeId);
//        task.setExecutionCallback(new ExecutionCallback[Int]() {
//            def done(future: Future[Int]): Unit = {
//                logger.debug("Task finished")
//
//                val taskTypeId = future.get()
//                if (taskTypeId == 1) {
//                    activeDirCount.decrementAndGet()
//                } else {
//                    activeFileCount.decrementAndGet()
//                }
//                activeAllCount.decrementAndGet()
//                if (shouldShutdown) {
//                    executorFinished()
//                }
//            }
//        });

        logger.debug("Task sent to executor: %s".format(f))
        if (isDir) {
            dirExecutor.submit[Int](runnable, future)
//            dirExecutor.execute(task)
        } else {
            fileExecutor.submit[Int](runnable, future)
//            fileExecutor.execute(task)
        }
    }

    /** Shutdown the complete hazelcast system.
      */
    override def quit(): Unit = {
        Hazelcast.shutdownAll()
    }

    /** Until all files and directories are processed
      */
    def waitUntilFinished(): Unit = {
        lock.synchronized {
            while (!finished) {
                lock.wait()
            }
        }
        logger.debug("Shutdown")
        dirExecutor.shutdown()
        fileExecutor.shutdown()
    }

    private def executorFinished(): Unit = {
        lock.synchronized {
            logger.debug("Finished")

            finished = true
            lock.notifyAll()

            logger.debug("Notification done")
        }
    }

    /** Report the current state of the dispatcher to the user
      */
    def report(): Unit = {
        val secs = (System.currentTimeMillis() - startTime) / 1000
        if (secs > 0) {
            val mbs = FileProcessor.totalRead.get() / secs
            logger.info(
              "%s: Files: local %d, local data %s (%s/s), local active: %d, cluster open %d, directories: local %d, local active: %d, cluster open %d"
                  .format(
                    memberId,
                    FileProcessor.totalCount.get(),
                    StorageUnit(FileProcessor.totalRead.get()),
                    StorageUnit(mbs),
                    FileProcessor.activeCount.get(),
                    activeFileCount.get(),
                    DirectoryProcessor.totalCount.get(),
                    DirectoryProcessor.activeCount.get(),
                    activeDirCount.get()
                  )
            )
        }
    }
}
