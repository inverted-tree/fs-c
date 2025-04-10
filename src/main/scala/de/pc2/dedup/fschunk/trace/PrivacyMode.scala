package de.pc2.dedup.fschunk.trace

import de.pc2.dedup.fschunk.trace
import org.apache.commons.codec.digest.DigestUtils

trait PrivacyMode {
    /* Encodes a filename to a privacy-preserving hash
     */
    def encodeFilename(file: String): String
}

/** Enumeration for the trace privacy mode
  *   - None: Emits the full path
  *   - Reversible: Emits a reversibly hashed full path
  *   - Hash: SHA-1 full path hashing
  *   - DirHash: Directory-level based SHA-1
  */
object PrivacyMode extends Enumeration {
    object None extends PrivacyMode {
        def encodeFilename(file: String): String = file
    }

    object Reversible extends PrivacyMode {
        def encodeFilename(file: String): String = file.hashCode.toString
    }

    object Hash extends PrivacyMode {
        def encodeFilename(file: String): String =
            DigestUtils.sha1Hex(file)
    }

    object DirHash extends PrivacyMode {
        def encodeFilename(file: String): String =
            file.split("/").map(DigestUtils.sha1Hex).mkString("/")
    }
}
