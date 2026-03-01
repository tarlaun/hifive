/*
 * Copyright 2022 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.davinci

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{Feature, GeometryType, IFeature}
import edu.ucr.cs.bdlab.beast.util.BitArray
import edu.ucr.cs.bdlab.davinci.IntermediateVectorTile._
import org.apache.spark.sql.Row
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.locationtech.jts.simplify.{DouglasPeuckerSimplifier, TopologyPreservingSimplifier}
import org.opengis.referencing.operation.MathTransform

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * PERFORMANCE CHANGE (requested):
 *  - Avoid computing expensive metrics (pixelCount, bytesEst, cellsCount) unless the CURRENT policy needs them.
 *  - "pixels" priority -> computes pixelCount (expensive) ONLY then.
 *  - "kb" capacity -> would require bytesEst (+ usually pixelCount). If you are not using kb, you won't pay for it.
 *  - "cells" capacity or "tuple" priority -> computes cellsCount only then.
 *  - "bytes" priority -> computes bytesEst only then (no pixelCount needed).
 *
 * Correctness fixes preserved:
 *  (Fix #1) robust non-null cell counting w/o brittle Row-casts
 *  (Fix #2) cells weight >= 1
 *  (Fix) sampler "none" truly disables sampling
 */
@DefaultSerializer(classOf[IntermediateVectorTileSerializer])
class IntermediateVectorTile(private[davinci] val resolution: Int,
                             private[davinci] val buffer: Int,
                             private[davinci] val zoom: Int,
                             @transient private[davinci] val dataToImage: MathTransform = null) extends Serializable {

  // ---------------- Internal helper for raster rings ----------------
  private case class Vertex(var x: Int, var y: Int, var next: Vertex = null) { var visited: Boolean = false }

  // ---------------- Storage (legacy buffers) ----------------
  // In sort-mode, selected subset lives in heap; these buffers are materialized on demand for compatibility.
  var features: ArrayBuffer[IFeature] = new ArrayBuffer[IFeature]()
  var geometries: ArrayBuffer[LiteGeometry] = new ArrayBuffer[LiteGeometry]()
  var numPoints: Int = 0

  // ---------------- Stats / flags ----------------
  var totalFeaturesSeen: Long = 0L
  var tileSizeLimitReached: Boolean = false
  var firsttime: Boolean = true

  var interiorPixels: BitArray = new BitArray((resolution + (2 * buffer)) * (resolution + (2 * buffer)))
  var boundaryPixels: BitArray = new BitArray((resolution + (2 * buffer)) * (resolution + (2 * buffer)))

  var useDouglasPeuckerSimplifier: Boolean = true
  var useTopologyPreservingSimplifier: Boolean = false //better

  var removeOverlappingFeatures: Boolean = false
  var dropAttributes: Boolean = false
  var dropThreshold: Int = 20
  var tolerance: Double = 100.0
  var changedBits: Int = 1

  @transient var reducedTile: VectorTile.Tile = null

  // ---------------- Debug ----------------
  @transient private lazy val DEBUG_TILE_ADD: Boolean =
  java.lang.Boolean.parseBoolean(System.getProperty("davinci.debug.tileAdd", "false")) ||
    sys.env.get("DAVINCI_DEBUG_TILEADD").exists(_.equalsIgnoreCase("true"))

  @inline private def dbg(msg: => String): Unit = {
    if (DEBUG_TILE_ADD) println(s"[TILEDBG z=$zoom res=$resolution buf=$buffer] $msg")
  }

  // =============================================================================
  // Sampler config (single source of truth)
  // =============================================================================

  // mode: "random" | "sort"
  private var samplerMode: String = "sort"

  // priority: "bytes" | "vertices" | "tuple" | "pixels"
  // direction: pixels default to smallest; others default to largest
  private var priority: String = "vertices"
  private var keepLargest: Boolean = true

  // capacity: "rows" | "kb" | "vertices" | "cells"
  private var capacityType: String = "cells"
  private var capacityLimit: Long = 100000L // meaning depends on capacityType

  // Public flags for serializer compatibility (kept in sync)
  var useSampling: Boolean = true //false
  //var samplingMethod = "none"
  var samplingMethod: String = "mode=sort priority=vertices:largest capacity=cells:10000000" // ""

  // =============================================================================
  // Lazy-metric policy helpers (PERF)
  // =============================================================================

  @inline private def needsCellsForPolicy: Boolean =
    (priority == "tuple") || (capacityType == "cells")

  @inline private def needsBytesForPolicy: Boolean =
    (priority == "bytes") || (capacityType == "kb")

  @inline private def needsPixelsForPolicy: Boolean =
    (priority == "pixels") || (capacityType == "kb")

  // =============================================================================
  // Heap-based sampler (mode=sort)
  // =============================================================================

  private var seqGen: Long = 0L
  private var heapUsed: Long = 0L

  @inline private def better(a: HeapItem, b: HeapItem): Boolean = {
    if (keepLargest) {
      if (a.score != b.score) a.score > b.score
      else if (a.tiePoints != b.tiePoints) a.tiePoints > b.tiePoints
      else a.seq < b.seq
    } else {
      if (a.score != b.score) a.score < b.score
      else if (a.tiePoints != b.tiePoints) a.tiePoints < b.tiePoints
      else a.seq < b.seq
    }
  }

  // Scala PriorityQueue is max-heap under Ordering; define "a > b" when "a is worse than b".
  private implicit val worstFirstOrdering: Ordering[HeapItem] = new Ordering[HeapItem] {
    override def compare(a: HeapItem, b: HeapItem): Int = {
      val aBetter = better(a, b)
      val bBetter = better(b, a)
      if (aBetter && !bBetter) -1
      else if (bBetter && !aBetter) 1
      else 0
    }
  }

  // Package-private for serializer.
  private[davinci] val heap: mutable.PriorityQueue[HeapItem] =
    mutable.PriorityQueue.empty[HeapItem](worstFirstOrdering)

  @inline private def resetHeapState(): Unit = {
    heap.clear()
    heapUsed = 0L
    seqGen = 0L
  }

  // Serializer helpers
  private[davinci] def heapUsedValue: Long = heapUsed
  private[davinci] def seqGenValue: Long = seqGen

  private[davinci] def restoreHeap(items: Array[HeapItem], used: Long, seq: Long): Unit = {
    heap.clear()
    if (items != null) {
      var i = 0
      while (i < items.length) {
        val it = items(i)
        if (it != null) heap.enqueue(it)
        i += 1
      }
    }
    heapUsed = used
    seqGen = seq
  }

  // ---------------- Utility ----------------
  @inline private def isIdxValid(i: Int): Boolean = i >= 0 && i < features.length && i < geometries.length

  // =============================================================================
  // Sampling enable/disable logic
  // =============================================================================

  // =============================================================================
  // Efficient RANDOM reservoir sampler (mode=random)
  //  - Keep a bounded set using a heap keyed by a random priority.
  //  - Supports rows/cells/vertices/kb capacities using the same "heapUsed > limit => evict worst" logic.
  // =============================================================================

  @transient private lazy val rng = new java.util.SplittableRandom(
    java.lang.Long.getLong("davinci.sampler.seed", System.nanoTime())
  )

  // Random priority key:
  // - For unweighted (rows): key = U(0,1)
  // - For weighted (cells/vertices/kb): Efraimidis-Spirakis key = U^(1/w) (larger is better)
  @inline private def randomKeyForWeight(w: Long): Double = {
    val ww = if (w <= 0L) 1.0 else w.toDouble
    var u = rng.nextDouble()
    // guard against u == 0.0 to avoid pow(0, ...)
    if (u <= 0.0) u = java.lang.Double.MIN_VALUE
    math.pow(u, 1.0 / ww) // larger is better
  }

  // Ordering for RANDOM heap: we want to evict the WORST random key first (smallest key).
  private implicit val randomWorstFirstOrdering: Ordering[HeapItem] = new Ordering[HeapItem] {
    override def compare(a: HeapItem, b: HeapItem): Int = {
      // Scala PriorityQueue is a max-heap. We want "head" to be WORST (smallest key),
      // so we define a > b when a is WORSE => when a.score < b.score.
      if (a.score < b.score) 1
      else if (a.score > b.score) -1
      else {
        // stable tie-break: newer loses first
        java.lang.Long.compare(b.seq, a.seq)
      }
    }
  }

  // Separate heap for random mode to avoid conflicting "better()" semantics.
  private[davinci] val randomHeap: mutable.PriorityQueue[HeapItem] =
    mutable.PriorityQueue.empty[HeapItem](randomWorstFirstOrdering)

  private var randomHeapUsed: Long = 0L
  private var randomSeqGen: Long = 0L

  @inline private def resetRandomHeapState(): Unit = {
    randomHeap.clear()
    randomHeapUsed = 0L
    randomSeqGen = 0L
  }

  /**
   * Offer into RANDOM heap:
   * - Use weight for capacity accounting (rows/cells/vertices/kb)
   * - Use random key for replacement (true reservoir behavior)
   */
  private def randomOffer(feature: IFeature,
                          geom: LiteGeometry,
                          cellsCountRaw: Int,
                          vertsCountRaw: Int,
                          bytesEstRaw: Long,
                          pixelCountRaw: Int): Unit = {

    if (capacityLimit <= 0L) return
    if (feature == null || geom == null) return

    val vertsCount: Int = if (vertsCountRaw >= 0) vertsCountRaw else math.max(0, geom.numPoints)

    // ONLY compute what's needed for WEIGHT in this mode.
    val cellsForWeight: Int =
      if (capacityType == "cells") {
        if (cellsCountRaw >= 0) cellsCountRaw else nonNullAttributeCells(feature)
      } else 0

    val bytesForWeight: Long =
      if (capacityType == "kb") {
        if (bytesEstRaw >= 0L) bytesEstRaw
        else estimateRecordSizeBytes(feature, geom, 0) // no pixels needed for estimate
      } else 0L

    val w: Long = capacityWeightForCounts(cellsForWeight, vertsCount, bytesForWeight)
    if (w <= 0L || w > capacityLimit) return

    randomSeqGen += 1L
    val key = randomKeyForWeight(w)

    val item = HeapItem(
      feature = feature,
      geometry = geom,
      cellsCount = if (cellsCountRaw >= 0) cellsCountRaw else -1,
      vertsCount = vertsCount,
      bytesEst = if (bytesEstRaw >= 0L) bytesEstRaw else -1L,
      pixelCount = if (pixelCountRaw >= 0) pixelCountRaw else -1,
      score = key,               // RANDOM priority key
      weight = w,
      tiePoints = 0,
      seq = randomSeqGen
    )

    capacityType match {
      case "rows" =>
        val cap = capacityLimit.toInt
        if (cap <= 0) return
        if (randomHeap.size < cap) {
          randomHeap.enqueue(item)
        } else {
          val worst = randomHeap.head // worst random key
          // Replace if new key is better (larger)
          if (item.score > worst.score) {
            randomHeap.dequeue()
            randomHeap.enqueue(item)
          }
        }

      case _ =>
        randomHeap.enqueue(item)
        randomHeapUsed += item.weight
        while (randomHeap.nonEmpty && randomHeapUsed > capacityLimit) {
          val ev = randomHeap.dequeue() // removes worst key first
          randomHeapUsed -= ev.weight
        }
    }
  }

  /** Disable sampling completely (and clear heap). */
  /** Disable sampling completely (and clear heaps). */
  private def disableSampling(): Unit = {
    useSampling = false
    samplingMethod = "none"
    resetHeapState()
    resetRandomHeapState()
  }

  /** Enable sampling with current internal config (and clear heaps). */
  private def enableSampling(): Unit = {
    useSampling = true
    samplingMethod =
      s"mode=$samplerMode priority=$priority:${if (keepLargest) "largest" else "smallest"} capacity=$capacityType:$capacityLimit"
    resetHeapState()
    resetRandomHeapState()
  }

  /**
   * Parse and set sampler config.
   *
   * Accepted:
   *   - "none" (or empty) => disables sampling
   *   - "mode=sort priority=vertices:largest capacity=cells:100000" (etc.)
   */
  def setSampler(spec: String): Unit = {
    val s = if (spec == null) "" else spec.trim
    if (s.isEmpty || s.equalsIgnoreCase("none")) {
      disableSampling()
      return
    }

    val parts = s.split("[;,\\s]+").map(_.trim).filter(_.nonEmpty)

    var _mode: String = samplerMode
    var _priority: String = priority
    var _dirLargest: Boolean = keepLargest
    var _capT: String = capacityType
    var _capL: Long = capacityLimit

    parts.foreach { p =>
      val kv = p.split("=", 2).map(_.trim)
      if (kv.length == 2) {
        kv(0).toLowerCase match {
          case "mode" =>
            _mode = kv(1).toLowerCase match {
              case "random" => "random"
              case "sort"   => "sort"
              case other    => other
            }

          case "priority" =>
            val ps = kv(1).toLowerCase.split(":", 2)
            _priority = ps(0) match {
              case "pixels" | "vertices" | "tuple" | "bytes" => ps(0)
              case other => other
            }
            // Direction defaults: pixels -> smallest, others -> largest
            _dirLargest =
              ps.lift(1).exists(_.trim == "largest") ||
                (!_priority.equals("pixels") && !ps.lift(1).contains("smallest"))
            if (_priority.equals("pixels") && ps.length == 1) _dirLargest = false
            if (!_priority.equals("pixels") && ps.length == 1) _dirLargest = true

          case "capacity" =>
            val cs = kv(1).toLowerCase.split(":", 2)
            _capT = cs(0) match {
              case "rows" | "kb" | "vertices" | "cells" => cs(0)
              case other => other
            }
            _capL = cs.lift(1).flatMap(x => scala.util.Try(x.trim.toLong).toOption).getOrElse(_capL)

          case _ => ()
        }
      }
    }

    samplerMode = _mode
    priority = _priority
    keepLargest = _dirLargest
    capacityType = _capT
    capacityLimit = math.max(0L, _capL)
    // Random mode uses random keys (larger is better), priority/direction are irrelevant.
    if (samplerMode == "random") {
      keepLargest = true
      // keep "priority" as-is for logging, but it does not affect random sampling
    }

    enableSampling()

    enableSampling()
  }

  def setSamplingMethod(method: String): Unit = setSampler(method)

  private def loadSamplerFromCLI(): Unit = {
    val fromProp = System.getProperty("davinci.sampler")
    val fromEnv  = sys.env.get("DAVINCI_SAMPLER")
    val spec = if (fromProp != null && fromProp.trim.nonEmpty) fromProp else fromEnv.getOrElse("")
    if (spec != null && spec.trim.nonEmpty) {
      println(s"[SAMPLER] loading from ${if (fromProp != null) "system property" else "env"}: " + spec)
      setSampler(spec.trim)
    }
  }

  // Initialize according to default public flags/method (none => disabled)
  setSampler(samplingMethod)
  loadSamplerFromCLI()

  // =============================================================================
  // Tile MBR
  // =============================================================================

  @transient private var _tileMBR: PreparedGeometry = _
  def tileMBR(factory: GeometryFactory): PreparedGeometry = {
    if (_tileMBR == null) {
      val mbr = Array[Double](-buffer, -buffer, resolution + buffer, resolution + buffer)
      if (dataToImage != null) {
        val imageToData: MathTransform = dataToImage.inverse()
        imageToData.transform(mbr, 0, mbr, 0, 2)
      }
      val envelope = new Envelope(mbr(0), mbr(2), mbr(1), mbr(3))
      _tileMBR = PreparedGeometryFactory.prepare(factory.toGeometry(envelope))
    }
    _tileMBR
  }

  def isEmpty: Boolean = numPoints == 0 && interiorPixels == null
  def nonEmpty: Boolean = !isEmpty

  // =============================================================================
  // (Fix #1) Cells = non-null non-geometry attributes (schema-based, safe)
  // =============================================================================
  private def nonNullAttributeCells(feat: IFeature): Int = {
    if (feat == null) return 0
    try {
      val n = feat.length
      if (n <= 0) return 0

      // Prefer IFeature's iNonGeom iterator (avoids guessing geometry index)
      feat match {
        case f: IFeature =>
          var c = 0
          val it = f.iNonGeom.toIterator
          while (it.hasNext) {
            val i = it.next()
            if (i >= 0 && i < n && !f.isNullAt(i)) {
              val v = f.get(i)
              v match {
                case _: Geometry     => ()
                case _: LiteGeometry => ()
                case null            => ()
                case _               => c += 1
              }
            }
          }
          c
        case _ =>
          // Fallback: count all non-null that are not geometry-ish
          var c = 0
          var i = 0
          while (i < n) {
            if (!feat.isNullAt(i)) {
              val v = feat.get(i)
              v match {
                case _: Geometry     => ()
                case _: LiteGeometry => ()
                case null            => ()
                case _               => c += 1
              }
            }
            i += 1
          }
          c
      }
    } catch {
      case _: Throwable => 0
    }
  }

  // =============================================================================
  // Lightweight record-size estimator (bytes)
  // NOTE: now pixelCount is OPTIONAL (pass 0 when you aren't using pixels/kb).
  // =============================================================================
  private def estimateRecordSizeBytes(feat: IFeature, geom: LiteGeometry, pixelCount: Int): Long = {
    /*if (feat == null || geom == null) return 0
    val vb = VectorTile.Tile.newBuilder()
    val lb = new VectorLayerBuilder(resolution, "features")
    lb.addFeature(feat, geom)
    vb.addLayers(lb.build())
    val total = vb.build().getSerializedSize
    //println("KB ENABLED: ", total)
    return total */


    val geomBytes = 8L * math.max(0, geom.numPoints) + 16L
    var attrBytes = 0L
    try {
      val n = feat.length
      var i = 0
      while (i < n) {
        if (!feat.isNullAt(i)) {
          feat.get(i) match {
            case _: Geometry     => ()
            case _: LiteGeometry => ()
            case s: String       => attrBytes += 12L + s.length
            case _: java.lang.Integer => attrBytes += 4
            case _: java.lang.Long    => attrBytes += 8
            case _: java.lang.Double  => attrBytes += 8
            case _: java.lang.Float   => attrBytes += 4
            case _: java.lang.Boolean => attrBytes += 1
            case _: java.lang.Short   => attrBytes += 2
            case _: java.lang.Byte    => attrBytes += 1
            case _: java.math.BigDecimal => attrBytes += 16
            case _: java.sql.Timestamp   => attrBytes += 12
            case _: java.sql.Date        => attrBytes += 8
            case arr: scala.collection.Seq[_]  => attrBytes += 8 + 4L * arr.size
            case m: scala.collection.Map[_, _] => attrBytes += 16L + 8L * m.size
            case _ => attrBytes += 12
          }
        }
        i += 1
      }
    } catch { case _: Throwable => () }

    // If you don't compute pixelCount, overhead becomes a small constant.
    val overhead = 24L + (pixelCount.toLong min 1024L) / 4L
    val total = geomBytes + attrBytes + overhead
    if (total <= 0L) 32L else total
  }

  // =============================================================================
  // Accurate pixel counting (unchanged) -- expensive, now only called if needed
  // =============================================================================
  private def calculateAccuratePixelCount(geometry: LiteGeometry): Int = {
    val dim = resolution + 2 * buffer
    val interior = new BitArray(dim * dim)
    val boundary = new BitArray(dim * dim)

    def inBounds(x: Int, y: Int): Boolean =
      x >= -buffer && x < resolution + buffer &&
        y >= -buffer && y < resolution + buffer

    def markInterior(x: Int, y: Int): Unit = {
      if (inBounds(x, y)) {
        val idx = (y + buffer) * dim + (x + buffer)
        interior.set(idx, true)
      }
    }
    def markBoundary(x: Int, y: Int): Unit = {
      if (inBounds(x, y)) {
        val idx = (y + buffer) * dim + (x + buffer)
        boundary.set(idx, true)
      }
    }

    geometry match {
      case p: LitePoint =>
        markBoundary(p.x, p.y)

      case mp: LiteMultiPoint =>
        var i = 0
        while (i < mp.numPoints) { markBoundary(mp.xs(i), mp.ys(i)); i += 1 }

      case ls: LiteLineString =>
        ls.parts.foreach { part =>
          var x1 = part.xs(0).toInt
          var y1 = part.ys(0).toInt
          val x2 = part.xs.last.toInt
          val y2 = part.ys.last.toInt
          val dx = math.abs(x2 - x1)
          val dy = math.abs(y2 - y1)
          val incx = if (x1 < x2) 1 else -1
          val incy = if (y1 < y2) 1 else -1

          if (dx > dy) {
            var p = 2 * dy - dx
            while (x1 != x2) {
              markBoundary(x1, y1)
              if (p >= 0) { y1 += incy; p -= 2 * dx }
              p += 2 * dy; x1 += incx
            }
          } else {
            var p = 2 * dx - dy
            while (y1 != y2) {
              markBoundary(x1, y1)
              if (p >= 0) { x1 += incx; p -= 2 * dy }
              p += 2 * dx; y1 += incy
            }
          }
          markBoundary(x2, y2)
        }

      case poly: LitePolygon =>
        val y0 = math.max(-buffer, poly.envelope.y.toInt)
        val y1 = math.min(resolution + buffer - 1, poly.envelope.getMaxY.toInt)
        var yy = y0
        while (yy <= y1) {
          val it = findPixelsInside(yy, poly)
          while (it.hasNext) markInterior(it.next(), yy)
          yy += 1
        }
        poly.parts.foreach { ring =>
          var x1 = ring.xs(0).toInt
          var y1 = ring.ys(0).toInt
          val x2 = ring.xs.last.toInt
          val y2 = ring.ys.last.toInt
          val dx = math.abs(x2 - x1)
          val dy = math.abs(y2 - y1)
          val incx = if (x1 < x2) 1 else -1
          val incy = if (y1 < y2) 1 else -1
          if (dx > dy) {
            var p = 2 * dy - dx
            while (x1 != x2) {
              markBoundary(x1, y1)
              if (p >= 0) { y1 += incy; p -= 2 * dx }
              p += 2 * dy; x1 += incx
            }
          } else {
            var p = 2 * dx - dy
            while (y1 != y2) {
              markBoundary(x1, y1)
              if (p >= 0) { x1 += incx; p -= 2 * dy }
              p += 2 * dx; y1 += incy
            }
          }
          markBoundary(x2, y2)
        }

      case _ => ()
    }

    var count = 0
    val total = dim * dim
    var i = 0
    while (i < total) { if (interior.get(i) || boundary.get(i)) count += 1; i += 1 }
    count
  }

  // =============================================================================
  // Rasterization helpers (unchanged)
  // =============================================================================
  private def rasterizeLineStringBoundary(linestring: LiteList): (Boolean, Int) = {
    var changed = false
    var changesCount = 0
    var i = 1
    while (i < linestring.numPoints) {
      var x1 = linestring.xs(i - 1).toInt
      var y1 = linestring.ys(i - 1).toInt
      val x2 = linestring.xs(i).toInt
      val y2 = linestring.ys(i).toInt
      val dx = (x2 - x1).abs
      val dy = (y2 - y1).abs
      val incx = if (x1 < x2) 1 else -1
      val incy = if (y1 < y2) 1 else -1
      if (dx > dy) {
        var p = 2 * dy - dx
        while (x1 != x2) {
          if (setPixelBoundary(x1, y1)) { changed = true; changesCount += 1 }
          if (p >= 0) { y1 += incy; p -= 2 * dx }
          p += 2 * dy; x1 += incx
        }
      } else {
        var p = 2 * dx - dy
        while (y1 != y2) {
          if (setPixelBoundary(x1, y1)) { changed = true; changesCount += 1 }
          if (p >= 0) { x1 += incx; p -= 2 * dy }
          p += 2 * dx; y1 += incy
        }
      }
      if (setPixelBoundary(x2, y2)) { changed = true; changesCount += 1 }
      i += 1
    }
    (changed, changesCount)
  }

  private def rasterizeGeometry(geometry: LiteGeometry): Int = {
    var changes = 0
    geometry match {
      case p: LitePoint =>
        if (setPixelBoundary(p.x, p.y)) changes += 1

      case mp: LiteMultiPoint =>
        var i = 0; while (i < mp.numPoints) { if (setPixelBoundary(mp.xs(i), mp.ys(i))) changes += 1; i += 1 }

      case ls: LiteLineString =>
        ls.parts.foreach { part =>
          val (changed, count) = rasterizeLineStringBoundary(part)
          if (changed) changes += count
        }

      case poly: LitePolygon =>
        val mbr: java.awt.Rectangle = poly.envelope
        var y = mbr.getMinY.toInt max (-buffer)
        val yMax = mbr.getMaxY.toInt min (resolution + buffer - 1)
        while (y <= yMax) {
          val xs = findPixelsInside(y, poly)
          while (xs.hasNext) { if (setPixelInterior(xs.next(), y)) changes += 1 }
          y += 1
        }
        poly.parts.foreach { part =>
          val (changed, count) = rasterizeLineStringBoundary(part)
          if (changed) changes += count
        }
    }
    changes
  }

  // =============================================================================
  // Exact MVT size helpers (KB capacity for sort mode)
  // (Not used if capacityType != "kb"; left as-is)
  // =============================================================================
  @inline private def kbLimitBytes: Long = capacityLimit * 1024L

  private def tileSizeBytesForItems(items: collection.IndexedSeq[HeapItem]): Int = {
    val vb = VectorTile.Tile.newBuilder()
    val lb = new VectorLayerBuilder(resolution, "features")
    var i = 0
    while (i < items.length) {
      val it = items(i)
      if (it != null && it.feature != null && it.geometry != null) lb.addFeature(it.feature, it.geometry)
      i += 1
    }
    vb.addLayers(lb.build())
    vb.build().toByteArray.length
  }

  /** Greedy exact-KB pack in already best-first order. */
  private def packByExactKB(sorted: Array[HeapItem]): Array[HeapItem] = {
    if (capacityLimit <= 0L) return Array.empty[HeapItem]
    val chosen = new ArrayBuffer[HeapItem](sorted.length)
    var i = 0
    while (i < sorted.length) {
      val cand = sorted(i)
      if (cand != null) {
        chosen += cand
        val sizeNow = tileSizeBytesForItems(chosen)
        if (sizeNow > kbLimitBytes) chosen.remove(chosen.length - 1)
      }
      i += 1
    }
    chosen.toArray
  }

  // =============================================================================
  // Policy functions
  // =============================================================================
  @inline private def scoreForMetric(pixelCount: Int, cellsCount: Int, vertsCount: Int, bytesEst: Long): Double = {
    priority match {
      case "pixels"   => pixelCount.toDouble
      case "tuple"    => cellsCount.toDouble
      case "vertices" => vertsCount.toDouble
      case "bytes"    => bytesEst.toDouble
      case _          => vertsCount.toDouble
    }
  }

  // (Fix #2) cells weight is at least 1 so we never drop everything when cellsCount=0
  @inline private def capacityWeightForCounts(cellsCount: Int, vertsCount: Int, bytesEst: Long): Long = {
    capacityType match {
      case "rows"     => 1L
      case "cells"    => math.max(1, cellsCount).toLong
      case "vertices" => math.max(0, vertsCount).toLong
      case "kb"       => math.max(1L, (bytesEst + 1023L) / 1024L) // ceil KB
      case _          => 1L
    }
  }

  // =============================================================================
  // Heap admission logic
  // =============================================================================
  private def heapOfferItem(item: HeapItem): Unit = {
    if (capacityLimit <= 0L) return
    if (item == null || item.feature == null || item.geometry == null) return
    if (item.weight <= 0L) return
    if (item.weight > capacityLimit) return
    //println(s"[DEBUG] HEAP ADD: Current Heap Size: ${heap.size}, Feature Weight: ${item.weight}")

    capacityType match {
      case "rows" =>
        val cap = capacityLimit.toInt
        if (heap.size < cap) {
          heap.enqueue(item)
          heapUsed += 1L
        } else if (cap > 0 && heap.nonEmpty) {
          val worst = heap.head
          if (better(item, worst)) {
            heap.dequeue()
            heap.enqueue(item)
          }
        }

      case _ =>
        heap.enqueue(item)
        heapUsed += item.weight
        while (heap.nonEmpty && heapUsed > capacityLimit) {
          val ev = heap.dequeue()
          heapUsed -= ev.weight
        }
    }
  }

  /**
   * PERF: "raw" metrics can be passed as -1 (unknown) and will be computed ONLY if this tile's
   * current policy actually needs them.
   */
  private def heapOfferFromRaw(feature: IFeature,
                               geom: LiteGeometry,
                               cellsCountRaw: Int,
                               vertsCountRaw: Int,
                               bytesEstRaw: Long,
                               pixelCountRaw: Int): Unit = {
    if (capacityLimit <= 0L) return
    if (feature == null || geom == null) return

    val vertsCount: Int =
      if (vertsCountRaw >= 0) vertsCountRaw else math.max(0, geom.numPoints)

    // Compute only if needed by current policy
    val cellsCount: Int =
      if (cellsCountRaw >= 0) cellsCountRaw
      else if (needsCellsForPolicy) nonNullAttributeCells(feature)
      else 0

    val pixelCount: Int =
      if (pixelCountRaw >= 0) pixelCountRaw
      else if (needsPixelsForPolicy) {
        try calculateAccuratePixelCount(geom) catch { case _: Throwable => 0 }
      } else 0

    val bytesEst: Long =
      if (bytesEstRaw >= 0L) bytesEstRaw
      else if (needsBytesForPolicy) {
        // If we didn't compute pixelCount, we just pass 0 (cheap constant overhead).
        estimateRecordSizeBytes(feature, geom, if (needsPixelsForPolicy) pixelCount else 0)
      } else 0L

    val weight = capacityWeightForCounts(cellsCount, vertsCount, bytesEst)
    if (weight <= 0L || weight > capacityLimit) return

    seqGen += 1L
    val score = scoreForMetric(pixelCount, cellsCount, vertsCount, bytesEst)

    val item = HeapItem(
      feature = feature,
      geometry = geom,
      // store computed-or-not metrics:
      // if not needed, keep -1 so merge can compute later IF a different destination policy needs it.
      cellsCount = if (cellsCountRaw >= 0 || needsCellsForPolicy) cellsCount else -1,
      vertsCount = vertsCount,
      bytesEst = if (bytesEstRaw >= 0L || needsBytesForPolicy) bytesEst else -1L,
      pixelCount = if (pixelCountRaw >= 0 || needsPixelsForPolicy) pixelCount else -1,
      score = score,
      weight = weight,
      tiePoints = math.max(0, vertsCount),
      seq = seqGen
    )

    heapOfferItem(item)
  }

  /** Materialize heap contents into legacy buffers in best-first order. */
  private[davinci] def materializeFromHeapIntoBuffers(): Unit = {
    features.clear()
    geometries.clear()
    numPoints = 0

    if (heap.isEmpty) return

    val arr = heap.toArray
    java.util.Arrays.sort(
      arr.asInstanceOf[Array[AnyRef]],
      new java.util.Comparator[AnyRef] {
        override def compare(a0: AnyRef, b0: AnyRef): Int = {
          val a = a0.asInstanceOf[HeapItem]
          val b = b0.asInstanceOf[HeapItem]
          if (better(a, b) && !better(b, a)) -1
          else if (better(b, a) && !better(a, b)) 1
          else 0
        }
      }
    )

    var i = 0
    while (i < arr.length) {
      val it = arr(i)
      if (it != null && it.feature != null && it.geometry != null) {
        features += it.feature
        geometries += it.geometry
        numPoints += it.geometry.numPoints
      }
      i += 1
    }
  }

  // =============================================================================
  // Main ingestion
  // =============================================================================
  def addFeature(feature: IFeature): IntermediateVectorTile = {
    if (feature == null) return this
    if (feature.getGeometry == null) return this

    if (dataToImage != null) {
      val mbr = tileMBR(feature.getGeometry.getFactory)
      if (mbr.disjoint(feature.getGeometry)) return this
    }

    // simplify -> LiteGeometry
    val simplified: LiteGeometry = try {
      val g = feature.getGeometry
      val s: Geometry =
        if (useDouglasPeuckerSimplifier) DouglasPeuckerSimplifier.simplify(g, tolerance)
        else if (useTopologyPreservingSimplifier) TopologyPreservingSimplifier.simplify(g, tolerance)
        else g

      simplifyGeometry(s)
    } catch {
      case t: Throwable =>
        dbg(s"addFeature: SKIP simplify threw ${t.getClass.getSimpleName}: ${t.getMessage}")
        null
    }

    if (simplified == null) return this

    // optional overlap gate (does NOT require pixelCount/bytes/cells)
    if (removeOverlappingFeatures) {
      val delta = rasterizeGeometry(simplified)
      if (delta < changedBits) {
        totalFeaturesSeen += 1
        return this
      }
    }

    val vertsCount: Int = math.max(0, simplified.numPoints)

    // Only compute expensive things if POLICY needs them (sort mode),
    // OR if capacityType needs them for WEIGHT (random mode).
    val needCells = needsCellsForPolicy || (samplerMode == "random" && capacityType == "cells")
    val needBytes = needsBytesForPolicy || (samplerMode == "random" && capacityType == "kb")
    val needPixels = needsPixelsForPolicy // pixels never needed for random weight in this implementation

    val cellsCountRaw: Int = if (needCells) nonNullAttributeCells(feature) else -1
    val pixelCountRaw: Int =
      if (needPixels) {
        try calculateAccuratePixelCount(simplified) catch { case _: Throwable => 0 }
      } else -1
    val bytesEstRaw: Long =
      if (needBytes) estimateRecordSizeBytes(feature, simplified, 0) else -1L

    // Store pixelCount on feature ONLY if we computed it.
    val storedPixelForFeature: Int = if (pixelCountRaw >= 0) math.abs(pixelCountRaw) else 0
    val stored: IFeature = Feature.create(feature, null, storedPixelForFeature)

    // sampling disabled => pass-through
    if (!useSampling || samplingMethod == "none") {
      geometries += simplified
      features += stored
      numPoints += simplified.numPoints
      totalFeaturesSeen += 1
      return this
    }

    samplerMode match {
      case "random" =>
        // TRUE efficient reservoir sampling with replacement using randomHeap
        randomOffer(
          feature = stored,
          geom = simplified,
          cellsCountRaw = cellsCountRaw,
          vertsCountRaw = vertsCount,
          bytesEstRaw = bytesEstRaw,
          pixelCountRaw = pixelCountRaw
        )

      case "sort" =>
        heapOfferFromRaw(
          feature = stored,
          geom = simplified,
          cellsCountRaw = cellsCountRaw,
          vertsCountRaw = vertsCount,
          bytesEstRaw = bytesEstRaw,
          pixelCountRaw = pixelCountRaw
        )

      case _ =>
        // fallback to sort heap behavior
        heapOfferFromRaw(
          feature = stored,
          geom = simplified,
          cellsCountRaw = cellsCountRaw,
          vertsCountRaw = vertsCount,
          bytesEstRaw = bytesEstRaw,
          pixelCountRaw = pixelCountRaw
        )
    }

    totalFeaturesSeen += 1
    this
  }


  // =============================================================================
  // Merge (PRINCIPLED + LAZY)
  // =============================================================================
  def merge(other: IntermediateVectorTile): IntermediateVectorTile = {
    if (other == null) return this

    val thisPassThrough = (!useSampling || samplingMethod == "none")

    val otherHasSortHeap   = other.heap != null && other.heap.nonEmpty
    val otherHasRandomHeap = other.randomHeap != null && other.randomHeap.nonEmpty
    val otherBufNonEmpty   = other.features != null && other.geometries != null &&
      other.features.nonEmpty && other.geometries.nonEmpty

    dbg(
      s"merge: BEGIN this(mode=$samplerMode cap=$capacityType:$capacityLimit useSampling=$useSampling sortHeap=${heap.size} randHeap=${randomHeap.size}) " +
        s"other(mode=${other.samplerMode} cap=${other.capacityType}:${other.capacityLimit} useSampling=${other.useSampling} sortHeap=${if (other.heap==null) -1 else other.heap.size} randHeap=${if (other.randomHeap==null) -1 else other.randomHeap.size} buf=${if (other.features==null) -1 else other.features.size})"
    )

    def ingestItem(f: IFeature, g: LiteGeometry, cellsRaw: Int, vertsRaw: Int, bytesRaw: Long, pixRaw: Int): Unit = {
      if (f == null || g == null) return
      if (thisPassThrough) {
        features += f
        geometries += g
        numPoints += g.numPoints
      } else if (samplerMode == "random") {
        randomOffer(f, g, cellsRaw, vertsRaw, bytesRaw, pixRaw)
      } else {
        heapOfferFromRaw(f, g, cellsRaw, vertsRaw, bytesRaw, pixRaw)
      }
      totalFeaturesSeen += 1
    }

    // 1) other randomHeap
    if (otherHasRandomHeap) {
      val arr = other.randomHeap.toArray
      var i = 0
      while (i < arr.length) {
        val it = arr(i)
        if (it != null) ingestItem(it.feature, it.geometry, it.cellsCount, it.vertsCount, it.bytesEst, it.pixelCount)
        i += 1
      }
      return this
    }

    // 2) other sort heap
    if (otherHasSortHeap) {
      val arr = other.heap.toArray
      var i = 0
      while (i < arr.length) {
        val it = arr(i)
        if (it != null) ingestItem(it.feature, it.geometry, it.cellsCount, it.vertsCount, it.bytesEst, it.pixelCount)
        i += 1
      }
      return this
    }

    // 3) other buffers
    if (otherBufNonEmpty) {
      val n = math.min(other.features.length, other.geometries.length)
      var i = 0
      while (i < n) {
        val of = other.features(i)
        val og = other.geometries(i)
        if (of != null && og != null) {
          // raw metrics unknown here; will be computed lazily only if needed
          ingestItem(of, og, -1, math.max(0, og.numPoints), -1L, -1)
        }
        i += 1
      }
    }

    this
  }

  // =============================================================================
  // Simplification to LiteGeometry (kept as-is from your provided implementation)
  // =============================================================================
  private[davinci] def simplifyGeometry(geometry: Geometry): LiteGeometry = geometry.getGeometryType match {
    case GeometryType.PointName =>
      val coord: Coordinate = geometry.getCoordinate
      val pt = Array[Double](coord.x, coord.y)
      if (dataToImage != null) dataToImage.transform(pt, 0, pt, 0, 1)
      val x = pt(0).round.toShort; val y = pt(1).round.toShort
      if (isVisible(x, y)) new LitePoint(x, y) else null

    case GeometryType.LineStringName =>
      val coords: CoordinateSequence = geometry.asInstanceOf[LineString].getCoordinateSequence
      val parts = new ArrayBuffer[LiteGeometry]()
      val currentX = new ArrayBuffer[Short]()
      val currentY = new ArrayBuffer[Short]()

      def addPoint(_x: Double, _y: Double): Unit = {
        val x = _x.round.toShort; val y = _y.round.toShort
        if (currentX.isEmpty || currentX.last != x || currentY.last != y) { currentX.append(x); currentY.append(y) }
      }

      val pt0 = Array[Double](coords.getX(0), coords.getY(0))
      if (dataToImage != null) dataToImage.transform(pt0, 0, pt0, 0, 1)
      var x1 = pt0(0); var y1 = pt0(1)
      var startVisible = isVisible(x1, y1)
      if (startVisible) addPoint(x1, y1)

      var i = 1
      while (i < coords.size()) {
        val pt = Array[Double](coords.getX(i), coords.getY(i))
        if (dataToImage != null) dataToImage.transform(pt, 0, pt, 0, 1)
        val x2 = pt(0); val y2 = pt(1)
        val endVisible = isVisible(x2, y2)

        if (i == 1 || x2.round != x1.round || y2.round != y1.round) {
          if (startVisible && endVisible) {
            addPoint(x2, y2)
          } else if (startVisible && !endVisible) {
            val t = trimLineSegment(x1, y1, x2, y2)
            if (t != null) {
              addPoint(t._3, t._4)
              parts.append(if (currentX.length == 1) new LitePoint(currentX.head, currentY.head) else new LiteList(currentX.toArray, currentY.toArray))
              currentX.clear(); currentY.clear()
            }
          } else if (!startVisible && endVisible) {
            val t = trimLineSegment(x1, y1, x2, y2)
            if (t != null) { addPoint(t._1, t._2); addPoint(t._3, t._4) }
          } else {
            val t = trimLineSegment(x1, y1, x2, y2)
            if (t != null) {
              parts.append(new LiteList(Array(t._1.round.toShort, t._3.round.toShort), Array(t._2.round.toShort, t._4.round.toShort)))
            }
          }
          x1 = x2; y1 = y2; startVisible = endVisible
        }
        i += 1
      }
      if (currentX.nonEmpty) parts.append(if (currentX.size > 1) new LiteList(currentX.toArray, currentY.toArray) else new LitePoint(currentX.head, currentY.head))
      if (parts.isEmpty) null
      else {
        val allPoints = parts.forall(_.numPoints == 1)
        if (parts.length == 1 && allPoints) parts.head
        else if (allPoints) {
          val xs = parts.map(_.asInstanceOf[LitePoint].x).toArray
          val ys = parts.map(_.asInstanceOf[LitePoint].y).toArray
          new LiteMultiPoint(xs, ys)
        } else {
          new LiteLineString(parts.filter(_.numPoints > 1).map(_.asInstanceOf[LiteList]).toArray)
        }
      }

    case GeometryType.LinearRingName =>
      val coords: CoordinateSequence = geometry.asInstanceOf[LineString].getCoordinateSequence
      val pointsX = new ArrayBuffer[Short]()
      val pointsY = new ArrayBuffer[Short]()
      def addPoint(_x: Double, _y: Double): Unit = {
        val x = _x.round.toShort; val y = _y.round.toShort
        if (pointsX.isEmpty || pointsX.last != x || pointsY.last != y) { pointsX.append(x); pointsY.append(y) }
      }
      def traceTileEdge(x1: Short, y1: Short, x2: Short, y2: Short, cw: Boolean): Unit = {
        var cx = x1; var cy = y1
        val ex = x2; val ey = y2
        if (cw) {
          while ({ val nxt = nextPointCWOrdering(cx, cy, ex, ey); cx = nxt._1; cy = nxt._2; addPoint(cx, cy); (cx != ex || cy != ey) }) ()
        } else {
          while ({ val nxt = nextPointCCWOrdering(cx, cy, ex, ey); cx = nxt._1; cy = nxt._2; addPoint(cx, cy); (cx != ex || cy != ey) }) ()
        }
      }
      val pt = Array[Double](coords.getX(0), coords.getY(0))
      if (dataToImage != null) dataToImage.transform(pt, 0, pt, 0, 1)
      var x1 = pt(0); var y1 = pt(1)
      var startVisible = isVisible(x1, y1)
      var sumForOrdering = 0.0
      var sumFirstPart = 0.0
      if (startVisible) addPoint(x1, y1)
      var i2 = 1
      while (i2 < coords.size()) {
        val p = Array[Double](coords.getX(i2), coords.getY(i2))
        try {
          if (dataToImage != null) dataToImage.transform(p, 0, p, 0, 1)
          val x2 = p(0); val y2 = p(1)
          val endVisible = isVisible(x2, y2)
          if (startVisible && endVisible) {
            addPoint(x2, y2)
          } else if (startVisible && !endVisible) {
            val t = trimLineSegment(x1, y1, x2, y2); if (t != null) { addPoint(t._3, t._4); sumForOrdering = (x2 - t._3.round) * (y2 + t._4.round) }
          } else if (!startVisible && endVisible) {
            val t = trimLineSegment(x1, y1, x2, y2)
            if (t != null) {
              sumForOrdering += (t._1.round - x1) * (t._2.round + y1)
              if (pointsX.nonEmpty) {
                sumForOrdering += (pointsX.last - t._1.round) * (pointsY.last + t._2.round)
                val cw = sumForOrdering > 0; traceTileEdge(pointsX.last, pointsY.last, t._1.round.toShort, t._2.round.toShort, cw)
              } else {
                addPoint(t._1, t._2); sumFirstPart = sumForOrdering
              }
              addPoint(x2, y2)
            }
          } else {
            val t = trimLineSegment(x1, y1, x2, y2)
            if (t == null) { sumForOrdering += (x2 - x1) * (y2 + y1) }
            else {
              if (pointsX.nonEmpty) {
                sumForOrdering += (t._1.round - x1) * (t._2.round + y1)
                val cw = sumForOrdering > 0; traceTileEdge(pointsX.last, pointsY.last, t._1.round.toShort, t._2.round.toShort, cw)
              } else {
                sumForOrdering += (t._1.round - x1) * (t._2.round + y1); sumFirstPart = sumForOrdering; addPoint(t._1, t._2)
              }
              addPoint(t._3, t._4); sumForOrdering = (x2 - t._3.round) * (y2 + t._4.round)
            }
          }
          x1 = x2; y1 = y2; startVisible = endVisible
        } catch { case _: org.geotools.referencing.operation.projection.ProjectionException => () }
        i2 += 1
      }
      if (!startVisible && pointsX.size == 1) { pointsX.clear(); pointsY.clear(); sumForOrdering += sumFirstPart }
      else if (!startVisible && pointsX.size > 1) {
        sumForOrdering += (pointsX.last - pointsX.head) * (pointsY.last + pointsY.head)
        val cw = (sumForOrdering + sumFirstPart) > 0
        traceTileEdge(pointsX.last, pointsY.last, pointsX.head, pointsY.head, cw)
      }
      if (pointsX.isEmpty) {
        val p0 = Array[Double](coords.getX(0), coords.getY(0))
        if (dataToImage != null) dataToImage.transform(p0, 0, p0, 0, 1)
        var xx1 = p0(0); var yy1 = p0(1); var inter = 0; var k = 1
        while (k < coords.size()) {
          p0(0) = coords.getX(k); p0(1) = coords.getY(k)
          if (dataToImage != null) dataToImage.transform(p0, 0, p0, 0, 1)
          val xx2 = p0(0); val yy2 = p0(1)
          if (yy1 < 0 && yy2 > 0 || yy2 < 0 && yy1 > 0) {
            val xInt = xx1 - yy1 * (xx2 - xx1) / (yy2 - yy1); if (xInt < 0) inter += 1
          }
          xx1 = xx2; yy1 = yy2; k += 1
        }
        if (inter % 2 == 1)
          new LiteList(Array(-buffer, -buffer, (resolution + buffer), (resolution + buffer), -buffer).map(_.toShort),
            Array(-buffer, (resolution + buffer), (resolution + buffer), -buffer, -buffer).map(_.toShort))
        else null
      } else if (pointsX.size == 1) new LitePoint(pointsX.head, pointsY.head)
      else new LiteList(pointsX.toArray, pointsY.toArray)

    case GeometryType.PolygonName =>
      val polygon: Polygon = geometry.asInstanceOf[Polygon]
      val parts = new Array[LiteGeometry](polygon.getNumInteriorRing + 1)
      parts(0) = simplifyGeometry(polygon.getExteriorRing)
      var numParts = 1
      if (parts(0) == null) null
      else if (parts(0).numPoints == 1) parts(0)
      else if (parts(0).numPoints <= 3) new LiteLineString(Array(parts(0).asInstanceOf[LiteList]))
      else {
        parts(0) match { case p: LiteList if p.isCCW => p.reverse(); case _ => () }
        var i = 0
        while (i < polygon.getNumInteriorRing) {
          val ringGeom = simplifyGeometry(polygon.getInteriorRingN(i))
          parts(numParts) = ringGeom
          if (ringGeom != null && ringGeom.numPoints > 3) {
            ringGeom match { case p: LiteList if p.isCW => p.reverse(); case _ => () }
            numParts += 1
          }
          i += 1
        }
        new LitePolygon(parts.slice(0, numParts).map(_.asInstanceOf[LiteList]))
      }

    case GeometryType.MultiPointName =>
      val npts = geometry.getNumPoints
      var pts = new Array[(Short, Short)](npts)
      val pt = Array(0.0, 0.0)
      var i = 0
      while (i < npts) {
        val coord = geometry.getGeometryN(i).getCoordinate
        pt(0) = coord.x; pt(1) = coord.y
        if (dataToImage != null) dataToImage.transform(pt, 0, pt, 0, 1)
        pts(i) = (pt(0).round.toShort, pt(1).round.toShort)
        i += 1
      }
      pts = pts.filter(p => isVisible(p._1, p._2)).sorted
      if (pts.isEmpty) null
      else if (pts.length == 1) new LitePoint(pts(0)._1, pts(0)._2)
      else {
        val xs = pts.map(_._1); val ys = pts.map(_._2)
        new LiteMultiPoint(xs, ys)
      }

    case GeometryType.MultiLineStringName =>
      val parts: Array[LiteGeometry] =
        (0 until geometry.getNumGeometries).map(i => simplifyGeometry(geometry.getGeometryN(i))).filter(_ != null).toArray
      LiteGeometry.combineGeometries(parts)

    case GeometryType.MultiPolygonName | GeometryType.GeometryCollectionName =>
      val parts: Array[LiteGeometry] =
        (0 until geometry.getNumGeometries).map(i => simplifyGeometry(geometry.getGeometryN(i))).filter(_ != null).toArray
      LiteGeometry.combineGeometries(parts)
  }

  // =============================================================================
  // Trim & visibility
  // =============================================================================
  private[davinci] def isRasterized: Boolean = false
  private[davinci] def shouldRasterize: Boolean = false

  @inline private def isVisible(x: Short, y: Short): Boolean =
    x >= -buffer && x <= resolution + buffer && y >= -buffer && y <= resolution + buffer
  @inline private def isVisible(x: Double, y: Double): Boolean =
    x >= -buffer && x <= resolution + buffer && y >= -buffer && y <= resolution + buffer

  private[davinci] def trimLineSegment(x1: Double, y1: Double, x2: Double, y2: Double):
  (Double, Double, Double, Double) = {
    if (isVisible(x1, y1) && isVisible(x2, y2)) return (x1, y1, x2, y2)
    var _x1 = x1; var _y1 = y1; var _x2 = x2; var _y2 = y2; var reversed = false
    def reverse(): Unit = { val tx = _x1; _x1 = _x2; _x2 = tx; val ty = _y1; _y1 = _y2; _y2 = ty; reversed = !reversed }
    if (_x1 > _x2) reverse()
    if (_x2 < -buffer) return null
    if (_x1 > resolution + buffer) return null
    if (_x1 < -buffer) { _y1 = ((-buffer - _x1) * _y2 + (_x2 + buffer) * _y1) / (_x2 - _x1); _x1 = -buffer }
    if (_x2 > resolution + buffer) { _y2 = ((resolution + buffer - _x1) * _y2 + (_x2 - resolution - buffer) * _y1) / (_x2 - _x1); _x2 = resolution + buffer }
    if (_y1 > _y2) reverse()
    if (_y2 < -buffer) return null
    if (_y1 > resolution + buffer) return null
    if (_y1 < -buffer) { _x1 = ((-buffer - _y1) * _x2 + (_y2 + buffer) * _x1) / (_y2 - _y1); _y1 = -buffer }
    if (_y2 > resolution + buffer) { _x2 = ((resolution + buffer - _y1) * _x2 + (_y2 - resolution - buffer) * _x1) / (_y2 - _y1); _y2 = resolution + buffer }
    if (reversed) reverse()
    (_x1, _y1, _x2, _y2)
  }

  // =============================================================================
  // VectorTile build
  // =============================================================================
  @transient private var _vectorTile: VectorTile.Tile = null
  def vectorTile: VectorTile.Tile = {
    if (_vectorTile == null) _vectorTile = buildVectorTile()
    _vectorTile
  }
  def vectorTile_=(newTile: VectorTile.Tile): Unit = { _vectorTile = newTile }

  private def buildVectorTile(): VectorTile.Tile = {
    try {
      val vb = VectorTile.Tile.newBuilder()
      if (!this.isRasterized) {
        val lb = new VectorLayerBuilder(resolution, "features")

        // ROBUSTNESS: if heap has content, use it (even if flags drift).
        val haveSortHeap   = heap != null && heap.nonEmpty
        val haveRandomHeap = randomHeap != null && randomHeap.nonEmpty

        if (haveRandomHeap && useSampling && samplingMethod != "none" && samplerMode == "random") {
          val cand = randomHeap.toArray
          // Sort by random key descending (best first)
          java.util.Arrays.sort(
            cand.asInstanceOf[Array[AnyRef]],
            new java.util.Comparator[AnyRef] {
              override def compare(a0: AnyRef, b0: AnyRef): Int = {
                val a = a0.asInstanceOf[HeapItem]
                val b = b0.asInstanceOf[HeapItem]
                // higher score (random key) is better
                java.lang.Double.compare(b.score, a.score)
              }
            }
          )
          val chosen = cand.asInstanceOf[Array[HeapItem]]
          var i = 0
          while (i < chosen.length) {
            val it = chosen(i)
            if (it != null && it.feature != null && it.geometry != null) lb.addFeature(it.feature, it.geometry)
            i += 1
          }

        } else if (haveSortHeap) {
          val cand = heap.toArray
          java.util.Arrays.sort(
            cand.asInstanceOf[Array[AnyRef]],
            new java.util.Comparator[AnyRef] {
              override def compare(a0: AnyRef, b0: AnyRef): Int = {
                val a = a0.asInstanceOf[HeapItem]
                val b = b0.asInstanceOf[HeapItem]
                if (better(a, b) && !better(b, a)) -1
                else if (better(b, a) && !better(a, b)) 1
                else 0
              }
            }
          )
          val sorted = cand.asInstanceOf[Array[HeapItem]]
          val canUseHeap = (samplerMode == "sort") && (samplingMethod != "none") && useSampling
          val chosen =
            if (canUseHeap && capacityType == "kb") packByExactKB(sorted)
            else sorted

          var i = 0
          while (i < chosen.length) {
            val it = chosen(i)
            if (it != null && it.feature != null && it.geometry != null) lb.addFeature(it.feature, it.geometry)
            i += 1
          }

        } else {
          val n2 = math.min(features.size, geometries.size)
          var i = 0
          while (i < n2) {
            if (features(i) != null && geometries(i) != null) lb.addFeature(features(i), geometries(i))
            i += 1
          }
        }

        vb.addLayers(lb.build())
        vb.build()
      } else {
        // raster tile path (unchanged)
        Seq(("interior", interiorPixels), ("boundary", boundaryPixels)).foreach { case (name, bmp) =>
          if (bmp != null && bmp.countOnes() > 0) {
            val lb = new VectorLayerBuilder(resolution, name)
            val rings = aggregateOccupiedPixels(bmp)
            lb.addFeature(null, new LitePolygon(rings))
            vb.addLayers(lb.build())
          }
        }
        vb.build()
      }
    } catch {
      case t: Throwable =>
        dbg(s"buildVectorTile: ERROR ${t.getClass.getSimpleName}: ${t.getMessage}")
        VectorTile.Tile.newBuilder().build()
    }
  }

  // =============================================================================
  // Legacy compat
  // =============================================================================
  def collectFeaturesFromSubtiles(): Unit = {
    if (!useSampling || samplingMethod == "none") return

    samplerMode match {
      case "sort" =>
        materializeFromHeapIntoBuffers()

      case "random" =>
        // Only materialize if some legacy caller expects buffers populated.
        features.clear()
        geometries.clear()
        numPoints = 0

        if (randomHeap != null && randomHeap.nonEmpty) {
          val arr = randomHeap.toArray
          java.util.Arrays.sort(
            arr.asInstanceOf[Array[AnyRef]],
            new java.util.Comparator[AnyRef] {
              override def compare(a0: AnyRef, b0: AnyRef): Int = {
                val a = a0.asInstanceOf[HeapItem]
                val b = b0.asInstanceOf[HeapItem]
                java.lang.Double.compare(b.score, a.score)
              }
            }
          )
          val chosen = arr.asInstanceOf[Array[HeapItem]]
          var i = 0
          while (i < chosen.length) {
            val it = chosen(i)
            if (it != null && it.feature != null && it.geometry != null) {
              features += it.feature
              geometries += it.geometry
              numPoints += it.geometry.numPoints
            }
            i += 1
          }
        }

      case _ => ()
    }
  }

  def adjustReservoirSizes(): Unit = ()
  def getNeighborIndices(subtileIndex: Int): Seq[Int] = Seq.empty

  // =============================================================================
  // Copy
  // =============================================================================
  def copy(): IntermediateVectorTile = {
    val newTile = new IntermediateVectorTile(this.resolution, this.buffer, this.zoom, this.dataToImage)

    // sampler config
    newTile.useSampling = this.useSampling
    newTile.samplingMethod = this.samplingMethod
    newTile.samplerMode = this.samplerMode
    newTile.priority = this.priority
    newTile.keepLargest = this.keepLargest
    newTile.capacityType = this.capacityType
    newTile.capacityLimit = this.capacityLimit

    if (this.heap != null && this.heap.nonEmpty) {
      val arr = this.heap.toArray
      var used = 0L
      var maxSeq = 0L
      var i = 0
      while (i < arr.length) {
        val it = arr(i)
        if (it != null) {
          newTile.heap.enqueue(it)
          used += (if (newTile.capacityType == "rows") 1L else it.weight)
          if (it.seq > maxSeq) maxSeq = it.seq
        }
        i += 1
      }
      newTile.heapUsed = used
      newTile.seqGen = maxSeq
      newTile.materializeFromHeapIntoBuffers()
    } else {
      newTile.features ++= this.features.map(_.copy().asInstanceOf[IFeature])
      newTile.geometries ++= this.geometries.map(_.copy())
      newTile.numPoints = this.numPoints
    }

    newTile.interiorPixels = this.interiorPixels.clone()
    newTile.boundaryPixels = this.boundaryPixels.clone()
    newTile.useDouglasPeuckerSimplifier = this.useDouglasPeuckerSimplifier
    newTile.useTopologyPreservingSimplifier = this.useTopologyPreservingSimplifier
    newTile.removeOverlappingFeatures = this.removeOverlappingFeatures
    newTile.dropAttributes = this.dropAttributes
    newTile.dropThreshold = this.dropThreshold
    newTile.tolerance = this.tolerance
    newTile.changedBits = this.changedBits
    newTile._tileMBR = this._tileMBR
    newTile.reducedTile = this.reducedTile
    newTile.totalFeaturesSeen = this.totalFeaturesSeen
    newTile
  }

  // =============================================================================
  // Raster helpers (unchanged below)
  // =============================================================================
  private def findPixelsInside(y: Int, polygon: LitePolygon): Iterator[Int] = {
    var intersections = new ArrayBuffer[Int]()
    val yCenter = y + 0.5
    var iRing = 0
    while (iRing <= polygon.numInteriorRings) {
      val ring: LiteList = if (iRing == 0) polygon.exteriorRing else polygon.interiorRing(iRing - 1)
      var i = 1
      while (i < ring.numPoints) {
        val x1 = ring.xs(i - 1); val y1 = ring.ys(i - 1)
        val x2 = ring.xs(i); val y2 = ring.ys(i)
        if ((y1 <= yCenter && y2 >= yCenter) || (y2 <= yCenter && y1 >= yCenter)) {
          if (y2 != y1) {
            val xInt: Double = x1 + (x2.toDouble - x1) / (y2.toDouble - y1) * (yCenter - y1)
            val clipped = ((xInt.round.toInt) max (-buffer - 1)) min (resolution + buffer)
            intersections.append(clipped)
          }
        }
        i += 1
      }
      iRing += 1
    }
    intersections = intersections.sorted
    (intersections.indices by 2).iterator.flatMap { i => intersections(i) to intersections(i + 1) }
  }

  private def aggregateOccupiedPixels(occupiedPixels: BitArray): Array[LiteList] = {
    val scanlineSize: Int = resolution + (2 * buffer)
    val topVertices: Array[Vertex] = new Array[Vertex](scanlineSize + 1)
    var leftVertex: Vertex = null
    val corners = new ArrayBuffer[Vertex]()
    var offset: Int = pointOffset(-buffer, -buffer)
    var y = -buffer
    while (y <= resolution + buffer) {
      var x = -buffer
      while (x <= resolution + buffer) {
        val blocked0 = if (x > -buffer && y > -buffer && occupiedPixels.get(offset - scanlineSize - 1)) 1 else 0
        val blocked1 = if (x < resolution + buffer && y > -buffer && occupiedPixels.get(offset - scanlineSize)) 2 else 0
        val blocked2 = if (x > -buffer && y < resolution + buffer && occupiedPixels.get(offset - 1)) 4 else 0
        val blocked3 = if (x < resolution + buffer && y < resolution + buffer && occupiedPixels.get(offset)) 8 else 0
        val pixelType: Int = blocked0 | blocked1 | blocked2 | blocked3
        pixelType match {
          case 0 | 3 | 5 | 10 | 12 | 15 => // Do nothing
          case 1 =>
            val newVertex = Vertex(x, y, leftVertex)
            if (topVertices(x + buffer) != null) topVertices(x + buffer).next = newVertex
            topVertices(x + buffer) = null
            leftVertex = null
          case 2 =>
            val newVertex = Vertex(x, y, topVertices(x + buffer))
            leftVertex = newVertex
            topVertices(x + buffer) = null
          case 4 =>
            val newVertex = Vertex(x, y, null)
            if (leftVertex != null) leftVertex.next = newVertex
            topVertices(x + buffer) = newVertex
            leftVertex = null
          case 6 =>
            val newVertex1 = Vertex(x, y, topVertices(x + buffer))
            val newVertex2 = Vertex(x, y, null)
            if (leftVertex != null) leftVertex.next = newVertex2
            leftVertex = newVertex1
            topVertices(x + buffer) = newVertex2
          case 7 =>
            val newVertex = Vertex(x, y, null)
            topVertices(x + buffer) = newVertex
            leftVertex = newVertex
            corners.append(newVertex)
          case 8 =>
            val newVertex = Vertex(x, y, null)
            topVertices(x + buffer) = newVertex
            leftVertex = newVertex
            corners.append(newVertex)
          case 9 =>
            val newVertex1 = Vertex(x, y, leftVertex)
            val newVertex2 = Vertex(x, y, null)
            if (topVertices(x + buffer) != null) topVertices(x + buffer).next = newVertex1
            leftVertex = newVertex2
            topVertices(x + buffer) = newVertex2
            corners.append(newVertex2)
          case 11 =>
            val newVertex = Vertex(x, y, leftVertex)
            topVertices(x + buffer) = newVertex
            leftVertex = null
          case 13 =>
            val newVertex = Vertex(x, y, null)
            if (topVertices(x + buffer) != null) topVertices(x + buffer).next = newVertex
            leftVertex = newVertex
            topVertices(x + buffer) = null
          case 14 =>
            val newVertex = Vertex(x, y, topVertices(x + buffer))
            if (leftVertex != null) leftVertex.next = newVertex
            leftVertex = null
            topVertices(x + buffer) = null
        }
        offset += 1
        x += 1
      }
      offset = offset - (resolution + (2 * buffer) + 1) + scanlineSize
      y += 1
    }

    val rings = new ArrayBuffer[LiteList]()
    var c = 0
    while (c < corners.length) {
      val corner = corners(c)
      if (corner != null && !corner.visited) {
        var cnt = 0
        var p = corner
        do { p = p.next; cnt += 1 } while (p != corner)
        val xs = new Array[Short](cnt + 1)
        val ys = new Array[Short](cnt + 1)
        p = corner; cnt = 0
        do {
          xs(cnt) = p.x.toShort; ys(cnt) = p.y.toShort; p.visited = true
          p = p.next; cnt += 1
        } while (p != corner)
        xs(cnt) = p.x.toShort; ys(cnt) = p.y.toShort
        rings.append(new LiteList(xs, ys))
      }
      c += 1
    }
    rings.toArray
  }

  def setPixelInterior(coordinateX: Double, coordinateY: Double): Boolean = {
    val x = coordinateX.round.toInt
    val y = coordinateY.round.toInt
    val index = pointOffset(x, y)
    if (index != -1 && !interiorPixels.get(index)) { interiorPixels.set(index, true); return true }
    false
  }

  def setPixelBoundary(coordinateX: Double, coordinateY: Double): Boolean = {
    val x = coordinateX.round.toInt
    val y = coordinateY.round.toInt
    val index = pointOffset(x, y)
    if (index != -1 && !boundaryPixels.get(index)) { boundaryPixels.set(index, true); return true }
    false
  }

  def pointOffset(x: Int, y: Int): Int =
    if (!isWithinBoundary(x, y)) -1
    else ((y + buffer) * (resolution + (2 * buffer))) + (x + buffer)

  private def isWithinBoundary(coordinateX: Double, coordinateY: Double): Boolean = {
    val x = coordinateX.round.toInt
    val y = coordinateY.round.toInt
    x >= -buffer && x < resolution + buffer && y >= -buffer && y < resolution + buffer
  }

  private[davinci] def nextPointCWOrdering(x: Short, y: Short, xEnd: Short, yEnd: Short): (Short, Short) = {
    if (x == xEnd && y == yEnd) return (xEnd, yEnd)
    if (x == -buffer) {
      if (y == resolution + buffer) if (y == yEnd) (xEnd, yEnd) else ((resolution + buffer).toShort, y)
      else if (x == xEnd && yEnd >= y) (xEnd, yEnd) else (x, (resolution + buffer).toShort)
    } else if (x == resolution + buffer) {
      if (y == -buffer) if (y == yEnd) (xEnd, yEnd) else ((-buffer).toShort, y)
      else if (x == xEnd && yEnd <= y) (xEnd, yEnd) else (x, (-buffer).toShort)
    } else if (y == -buffer) {
      if (x == -buffer) if (x == xEnd) (xEnd, yEnd) else (x, (resolution + buffer).toShort)
      else if (y == yEnd && xEnd <= x) (xEnd, yEnd) else ((-buffer).toShort, y)
    } else {
      if (x == resolution + buffer) if (x == xEnd) (xEnd, yEnd) else (x, (-buffer).toShort)
      else if (y == yEnd && xEnd >= x) (xEnd, yEnd) else ((resolution + buffer).toShort, y)
    }
  }

  private[davinci] def nextPointCCWOrdering(x: Short, y: Short, xEnd: Short, yEnd: Short): (Short, Short) = {
    if (x == xEnd && y == yEnd) return (xEnd, yEnd)
    if (x == -buffer) {
      if (y == -buffer) if (y == yEnd) (xEnd, yEnd) else ((resolution + buffer).toShort, y)
      else if (x == xEnd && yEnd <= y) (xEnd, yEnd) else (x, (-buffer).toShort)
    } else if (x == resolution + buffer) {
      if (y == resolution + buffer) if (y == yEnd) (xEnd, yEnd) else ((-buffer).toShort, y)
      else if (x == xEnd && yEnd >= y) (xEnd, yEnd) else (x, (resolution + buffer).toShort)
    } else if (y == -buffer) {
      if (x == resolution + buffer) if (x == xEnd) (xEnd, yEnd) else (x, (resolution + buffer).toShort)
      else if (y == yEnd && xEnd >= x) (xEnd, yEnd) else ((resolution + buffer).toShort, y)
    } else {
      if (x == -buffer) if (x == xEnd) (xEnd, yEnd) else (x, (-buffer).toShort)
      else if (y == yEnd && xEnd <= x) (xEnd, yEnd) else ((-buffer).toShort, y)
    }
  }

  // =============================================================================
  // Getters/Setters (kept)
  // =============================================================================
  def getFeatures: ArrayBuffer[IFeature] = this.features
  def setFeatures(newFeatures: Seq[IFeature]): Unit = { this.features.clear(); this.features ++= newFeatures }
  def getGeometries: Seq[LiteGeometry] = this.geometries.toSeq
  def setGeometries(newGeometries: Seq[LiteGeometry]): Unit = { this.geometries.clear(); this.geometries ++= newGeometries }
  def addFeatureAndGeometry(feature: IFeature, geometry: LiteGeometry): Unit = { this.features.append(feature); this.geometries.append(geometry) }
  def getFeature(index: Int): Row = this.features(index)
  def getGeometry(index: Int): LiteGeometry = this.geometries(index)
  def setFeaturesAndGeometries(newFeatures: ArrayBuffer[IFeature], newGeometries: ArrayBuffer[LiteGeometry]): Unit = {
    this.features.clear(); this.features ++= newFeatures
    this.geometries.clear(); this.geometries ++= newGeometries
  }

  def calculateMetersPerPixel(webMercatorCoords: Array[Double], pixelDimensions: (Int, Int)): Double = {
    val (x1, y1, x2, y2) = (webMercatorCoords(0), webMercatorCoords(1), webMercatorCoords(2), webMercatorCoords(3))
    val distanceX = Math.abs(x2 - x1)
    val distanceY = Math.abs(y2 - y1)
    val averageDistance = (distanceX + distanceY) / 2
    val (pixelWidth, pixelHeight) = pixelDimensions
    val pixels = Math.max(pixelWidth, pixelHeight)
    averageDistance / pixels
  }

  def areBitmapsIdentical(bitmap1: BitArray, bitmap2: BitArray, tol: Int): Boolean = {
    if (bitmap1.size != bitmap2.size) return false
    var differences = 0
    var i = 0L
    while (i < bitmap2.size) {
      if (bitmap2.get(i) && !bitmap1.get(i)) {
        differences += 1
        if (differences > tol) return false
      }
      i += 1
    }
    true
  }
}

// ============================================================================
// Companion
// ============================================================================
object IntermediateVectorTile {

  /**
   * Heap item stored in sampler heap.
   *
   * LAZY metrics:
   *   - cellsCount / bytesEst / pixelCount may be -1 meaning "unknown".
   *   - merge() will keep them unknown unless the destination policy needs them.
   */
  private[davinci] case class HeapItem(
                                        feature: IFeature,
                                        geometry: LiteGeometry,
                                        cellsCount: Int,   // -1 if unknown
                                        vertsCount: Int,   // always available (cheap)
                                        bytesEst: Long,    // -1 if unknown
                                        pixelCount: Int,   // -1 if unknown
                                        score: Double,
                                        weight: Long,
                                        tiePoints: Int,
                                        seq: Long
                                      )

  /** The maximum number of points in one vector tile before we decide to rasterize */
  val maxPointsPerTile: Long = 20000L
  /** The maximum number of distinct features (geometries) to keep in one tile before rasterization */
  val maxFeaturesPerTile: Long = 1000L
}
