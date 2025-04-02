package de.pc2.dedup.fschunk.handler

import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.Reporting

/** Trait for handling of file data
  */
trait FileDataHandler extends Reporting {
    def handle(f: File): Unit

    def handle(fp: FilePart): Unit

    /** Empty default implementation
      */
    def quit(): Unit = {}

    def fileError(filename: String, fileSize: Long): Unit = {}

    def report(): Unit = {}
}
