package de.pc2.dedup.chunker

import de.pc2.dedup.chunker.fixed.FixedChunker
import de.pc2.dedup.chunker.rabin.RabinChunker

import java.nio.ByteBuffer

/** Trait of all chunker sessions.
  */
trait ChunkerSession {

    /** chunks "size" bytes of the data array. The session may contain data from
      * that array in an additinal open chunk.
      */
    def chunk(data: ByteBuffer)(h: (Chunk => Unit)): Unit

    /** Return additional chunks that stayed open after chunk calls. The chunker
      * session should not contain any state after calling close. However, it
      * should be ready to process new chunk calls.
      */
    def close()(h: (Chunk => Unit)): Unit

    /** name of the chunker
      */
    def chunkerName(): String
}

/** Trait for all chunker implementations. All thread bounded operations should
  * be performed in a chunker session instance.
  */
trait Chunker {

    /** Creates a new chunker session
      */
    def createSession(): ChunkerSession

    /** name of the chunker
      */
    def chunkerName(): String
}

object ChunkerFactory {
    def createChunker(
        chunkerType: String,
        logChunkHashes: Boolean,
        digestType: String,
        digestLength: Int,
        salt: Option[String]
    ): Chunker =
        chunkerType match {
            case "cdc2" =>
                new RabinChunker(
                  512,
                  2 * 1024,
                  8 * 1024,
                  logChunkHashes,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "c2"
                )
            case "cdc4" =>
                new RabinChunker(
                  1 * 1024,
                  4 * 1024,
                  16 * 1024,
                  logChunkHashes,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "c4"
                )
            case "cdc8" =>
                new RabinChunker(
                  2 * 1024,
                  8 * 1024,
                  32 * 1024,
                  logChunkHashes,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "c8"
                )
            case "cdc16" =>
                new RabinChunker(
                  4 * 1024,
                  16 * 1024,
                  64 * 1024,
                  logChunkHashes,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "c16"
                )
            case "cdc32" =>
                new RabinChunker(
                  8 * 1024,
                  32 * 1024,
                  128 * 1024,
                  logChunkHashes,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "c32"
                )
            case "cdc64" =>
                new RabinChunker(
                  16 * 1024,
                  64 * 1024,
                  256 * 1024,
                  logChunkHashes,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "c64"
                )
            case "fixed2" =>
                new FixedChunker(
                  2 * 1024,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "f2"
                )
            case "fixed4" =>
                new FixedChunker(
                  4 * 1024,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "f4"
                )
            case "fixed8" =>
                new FixedChunker(
                  8 * 1024,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "f8"
                )
            case "fixed16" =>
                new FixedChunker(
                  16 * 1024,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "f16"
                )
            case "fixed32" =>
                new FixedChunker(
                  32 * 1024,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "f32"
                )
            case "fixed64" =>
                new FixedChunker(
                  64 * 1024,
                  new DigestFactory(
                    digestType,
                    digestLength,
                    salt
                  ),
                  "f64"
                )
            case _ =>
                println("Cannot identify chunker type")
                sys.exit(1)
        }
}
