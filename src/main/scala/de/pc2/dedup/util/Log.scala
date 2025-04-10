package de.pc2.dedup.util

import org.apache.commons.logging._

class LoggerProxy(val logger: org.apache.commons.logging.Log) {
    def debug(msg: => Object): Unit = {
        if (logger.isDebugEnabled) {
            logger.debug(msg);
        }
    }

    def info(msg: => Object): Unit = {
        if (logger.isInfoEnabled) {
            logger.info(msg);
        }
    }

    def warn(msg: => Object): Unit = {
        if (logger.isWarnEnabled) {
            logger.warn(msg);
        }
    }

    def error(msg: => Object): Unit = {
        if (logger.isErrorEnabled) {
            logger.error(msg);
        }
    }

    def error(msg: => Object, t: java.lang.Throwable): Unit = {
        if (logger.isErrorEnabled) {
            logger.error(msg, t);
        }
    }

    def fatal(msg: Object): Unit = {
        logger.fatal(msg);
    }

    def fatal(msg: Object, t: java.lang.Throwable): Unit = {
        logger.fatal(msg, t);
    }
}

/** Provides easy log4j logging for scala classes. Based on this article:
  * https://www.uncarved.com/articles/LogHelper/
  */
trait Log {
    val loggerName: String = getLoggerName
    lazy val logger = new LoggerProxy(LogFactory.getLog(loggerName))

    private def getLoggerName: String = {
        val name = this.getClass.getName
        val i = name.indexOf("$")
        val name2 = if (i >= 0) {
            name.substring(0, i - 1)
        } else {
            name
        }
        val j = name2.lastIndexOf(".")
        val name3 = if (j >= 0) {
            name2.substring(j + 1)
        } else {
            name2
        }
        name3
    }
}
