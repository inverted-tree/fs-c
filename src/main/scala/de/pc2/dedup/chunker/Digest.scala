package de.pc2.dedup.chunker;

import java.math.BigInteger
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.Arrays
import java.nio.charset.Charset
import java.util

/** Compation object to create fingerprints of a chunk or a file
  */
class DigestFactory(
    val digestType: String,
    val digestLength: Int,
    val salt: Option[String]
) {

    lazy val saltArray: Option[Array[Byte]] = salt match {
        case Some(s) => Some(s.getBytes(Charset.forName("UTF-8")))
        case None    => None
    }

    /** Tests if the digest type is valid
      */
    def testDigestType(): Unit = {
        try {
            val md = MessageDigest.getInstance(digestType)
            if (digestLength > md.getDigestLength) {
                throw new IllegalArgumentException(
                  "Digest length larger than 20 not allowed"
                )
            }
        } catch {
            case e: NoSuchAlgorithmException =>
                throw new IllegalArgumentException("Digest type not known");
        }
    }
    testDigestType()

    /** Builder class for Digests
      */
    class DigestBuilder {
        val md: MessageDigest = MessageDigest.getInstance(digestType)

        /** Append new bytes to the current DigestBuilder
          */
        def append(buf: Array[Byte], pos: Int, len: Int): DigestBuilder = {
            if (len > 0) {
                md.update(buf, pos, len)
            }
            return this
        }

        /** Append new bytes from a ByteBuffer to the current DigestBuilder
          */
        def append(buf: ByteBuffer): DigestBuilder = {
            md.update(buf)
            return this
        }

        /** Create a new Digest from the current data and reset the
          * DigestBuilder
          */
        def build(): Digest = {
            saltArray match {
                case Some(sa) => md.update(sa)
                case None     => // no salting
            }
            val fullDigest = md.digest()
            val digest = if (digestLength == md.getDigestLength) {
                fullDigest
            } else {
                val d = new Array[Byte](digestLength)
                System.arraycopy(fullDigest, 0, d, 0, digestLength)
                d
            }
            md.reset()

            return new Digest(digest)
        }
    }

    /** Creates a new DigestBuilder
      */
    def builder(): DigestBuilder = {
        return new DigestBuilder()
    }
}

/** Fingerprint of a chunk or a file. The reason not to use a ByteArray directly
  * is that hashCode and equals has not the expected behavior on a ByteArray
  */
case class Digest(digest: Array[Byte]) {

    /** Hashcode of the digest. Calls Arrays.hashCode()
      */
    override def hashCode: Int = return util.Arrays.hashCode(digest)

    /** Checks if two digests are equal. Calls Array.equals()
      */
    override def equals(o: Any): Boolean = {
        o match {
            case Digest(fp) => util.Arrays.equals(this.digest, fp)
            case _          => false
        }
    }

    override def toString: String = {
        val bi = new BigInteger(1, digest)
        val result = bi.toString(16)
        if (result.length() % 2 != 0) {
            "0" + result
        } else {
            result
        }
    }
}
