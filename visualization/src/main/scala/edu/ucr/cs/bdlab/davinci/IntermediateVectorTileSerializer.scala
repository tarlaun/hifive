package edu.ucr.cs.bdlab.davinci

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.util.BitArray
import edu.ucr.cs.bdlab.davinci.IntermediateVectorTile.HeapItem

/**
 * Serializer aligned with the rewritten IntermediateVectorTile where HeapItem stores
 * per-item metrics so merge/build never needs to cast feature -> Row to recompute weights/scores.
 *
 * Expected HeapItem signature (in IntermediateVectorTile companion):
 *   case class HeapItem(
 *     feature: IFeature,
 *     geometry: LiteGeometry,
 *     score: Double,
 *     tiePoints: Int,
 *     weight: Long,
 *     seq: Long,
 *     cellsCount: Int,
 *     vertsCount: Int,
 *     bytesEst: Long,
 *     pixelCount: Int
 *   )
 */
class IntermediateVectorTileSerializer extends Serializer[IntermediateVectorTile] {

  override def write(kryo: Kryo, output: Output, t: IntermediateVectorTile): Unit = {
    // 1) Basic tile metadata
    output.writeInt(t.resolution)
    output.writeInt(t.buffer)
    output.writeInt(t.zoom)

    // 2) Sampler config (public vars)
    output.writeBoolean(t.useSampling)
    output.writeString(Option(t.samplingMethod).getOrElse(""))

    // 3) Misc flags / params
    output.writeLong(t.totalFeaturesSeen)
    output.writeBoolean(t.tileSizeLimitReached)
    output.writeBoolean(t.firsttime)

    output.writeBoolean(t.useDouglasPeuckerSimplifier)
    output.writeBoolean(t.useTopologyPreservingSimplifier)

    output.writeBoolean(t.removeOverlappingFeatures)
    output.writeBoolean(t.dropAttributes)
    output.writeInt(t.dropThreshold)
    output.writeDouble(t.tolerance)
    output.writeInt(t.changedBits)

    // 4) Bitmaps
    kryo.writeObject(output, t.interiorPixels)
    kryo.writeObject(output, t.boundaryPixels)

    // 5) Legacy buffers (kept for compatibility / older code paths)
    output.writeInt(t.numPoints)
    val n = math.min(
      if (t.features != null) t.features.size else 0,
      if (t.geometries != null) t.geometries.size else 0
    )
    output.writeInt(n)

    var i = 0
    while (i < n) { kryo.writeClassAndObject(output, t.features(i)); i += 1 }
    i = 0
    while (i < n) { kryo.writeClassAndObject(output, t.geometries(i)); i += 1 }

    // 6) Heap contents (authoritative for sort-mode)
    val heapArr: Array[HeapItem] =
      if (t.heap != null && t.heap.nonEmpty) t.heap.toArray else Array.empty[HeapItem]

    output.writeInt(heapArr.length)

    i = 0
    while (i < heapArr.length) {
      val it = heapArr(i)

      // payload
      kryo.writeClassAndObject(output, it.feature)
      kryo.writeClassAndObject(output, it.geometry)

      // ordering + capacity fields
      output.writeDouble(it.score)
      output.writeInt(it.tiePoints)
      output.writeLong(it.weight)
      output.writeLong(it.seq)

      // NEW: per-item metrics so merges can recompute without Row casts
      output.writeInt(it.cellsCount)
      output.writeInt(it.vertsCount)
      output.writeLong(it.bytesEst)
      output.writeInt(it.pixelCount)

      i += 1
    }

    // Persist counters exactly
    output.writeLong(t.heapUsedValue)
    output.writeLong(t.seqGenValue)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[IntermediateVectorTile]): IntermediateVectorTile = {
    // 1) Basic metadata
    val resolution = input.readInt()
    val buffer     = input.readInt()
    val zoom       = input.readInt()

    // Construct tile (dataToImage transient => null)
    val tile = new IntermediateVectorTile(resolution, buffer, zoom, null)

    // 2) Sampler config
    val useSampling = input.readBoolean()
    val samplingMethod = Option(input.readString()).getOrElse("")

    tile.useSampling = useSampling
    tile.samplingMethod = samplingMethod

    // Restore internal sampler policy (also resets heap ordering state based on spec)
    if (samplingMethod.nonEmpty && samplingMethod != "none") {
      tile.setSampler(samplingMethod)
    }
    // setSampler() forces useSampling=true; restore actual value:
    tile.useSampling = useSampling
    tile.samplingMethod = samplingMethod

    // 3) Misc flags / params
    tile.totalFeaturesSeen = input.readLong()
    tile.tileSizeLimitReached = input.readBoolean()
    tile.firsttime = input.readBoolean()

    tile.useDouglasPeuckerSimplifier = input.readBoolean()
    tile.useTopologyPreservingSimplifier = input.readBoolean()

    tile.removeOverlappingFeatures = input.readBoolean()
    tile.dropAttributes = input.readBoolean()
    tile.dropThreshold = input.readInt()
    tile.tolerance = input.readDouble()
    tile.changedBits = input.readInt()

    // 4) Bitmaps
    tile.interiorPixels = kryo.readObject(input, classOf[BitArray])
    tile.boundaryPixels = kryo.readObject(input, classOf[BitArray])

    // 5) Legacy buffers
    tile.numPoints = input.readInt()
    val n = input.readInt()

    var i = 0
    while (i < n) {
      val f = kryo.readClassAndObject(input).asInstanceOf[IFeature]
      tile.features.append(f)
      i += 1
    }

    i = 0
    while (i < n) {
      val g = kryo.readClassAndObject(input).asInstanceOf[LiteGeometry]
      tile.geometries.append(g)
      i += 1
    }

    // 6) Heap restore
    val heapCount = input.readInt()

    if (heapCount > 0) {
      val restored = new Array[HeapItem](heapCount)

      var j = 0
      while (j < heapCount) {
        val f = kryo.readClassAndObject(input).asInstanceOf[IFeature]
        val g = kryo.readClassAndObject(input).asInstanceOf[LiteGeometry]

        val score = input.readDouble()
        val tiePts = input.readInt()
        val weight = input.readLong()
        val seq = input.readLong()

        // NEW: per-item metrics
        val cellsCount = input.readInt()
        val vertsCount = input.readInt()
        val bytesEst   = input.readLong()
        val pixelCount = input.readInt()

        restored(j) = HeapItem(
          feature    = f,
          geometry   = g,
          score      = score,
          tiePoints  = tiePts,
          weight     = weight,
          seq        = seq,
          cellsCount = cellsCount,
          vertsCount = vertsCount,
          bytesEst   = bytesEst,
          pixelCount = pixelCount
        )
        j += 1
      }

      val heapUsed = input.readLong()
      val seqGen   = input.readLong()

      // Restore heap (no reflection)
      tile.restoreHeap(restored, heapUsed, seqGen)

      // CRITICAL: always materialize so older code paths / flag drift can't output empty tiles.
      tile.materializeFromHeapIntoBuffers()
    } else {
      // consume counters (still written even if heapCount==0)
      val _  = input.readLong()
      val __ = input.readLong()
    }

    tile
  }
}
