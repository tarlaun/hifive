package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.geolite.IFeature
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.index.strtree.STRtree

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object VASUtil {

  private val DEFAULT_BATCH_SIZE = 1000

  def proximityFunction(p1: (Double, Double), p2: (Double, Double), epsilon: Double): Double = {
    val dx = p1._1 - p2._1
    val dy = p1._2 - p2._2
    val distSq = dx * dx + dy * dy
    val distThreshold = 4.0 * epsilon
    if (distSq > distThreshold * distThreshold) 0.0
    else Math.exp(-distSq / (2.0 * epsilon * epsilon))
  }

  case class PointWithResponsibility(
                                      point: (Double, Double),
                                      var responsibility: Double,
                                      featureIndex: Int,
                                      geometryIndex: Int
                                    )

  def extractPoint(geom: LiteGeometry): (Double, Double) = geom match {
    case p: LitePoint => (p.x.toDouble, p.y.toDouble)
    case mp: LiteMultiPoint =>
      if (mp.numPoints > 0) (mp.xs(0).toDouble, mp.ys(0).toDouble) else (0.0, 0.0)
    case ls: LiteLineString =>
      if (ls.parts.nonEmpty && ls.parts(0).numPoints > 0) {
        val part = ls.parts(0)
        (part.xs(0).toDouble, part.ys(0).toDouble)
      } else (0.0, 0.0)
    case p: LitePolygon =>
      if (p.exteriorRing.numPoints > 0) {
        val ring = p.exteriorRing
        (ring.xs(0).toDouble, ring.ys(0).toDouble)
      } else (0.0, 0.0)
    case _ =>
      val env = geom.envelope
      ((env.getMinX + env.getMaxX) / 2, (env.getMinY + env.getMaxY) / 2)
  }

  def generateVASSample(
                         features: ArrayBuffer[IFeature],
                         geometries: ArrayBuffer[LiteGeometry],
                         sampleSize: Int,
                         epsilon: Double,
                         useLocalityOptimization: Boolean = true,
                         maxIterations: Int = 1
                       ): Array[Int] = {

    if (features.length <= sampleSize)
      return features.indices.toArray

    val dataset = features.indices.map(i => (i, i)).to[ArrayBuffer]
    val randomIndices = Random.shuffle(dataset.indices.toList).take(sampleSize)

    val pointsWithResp = new ArrayBuffer[PointWithResponsibility](sampleSize)
    for (idx <- randomIndices) {
      val (fIdx, gIdx) = dataset(idx)
      val point = extractPoint(geometries(gIdx))
      pointsWithResp.append(PointWithResponsibility(point, 0.0, fIdx, gIdx))
    }

    val batchSize = math.min(DEFAULT_BATCH_SIZE, pointsWithResp.length)
    for (batchStart <- 0 until pointsWithResp.length by batchSize) {
      val batchEnd = math.min(batchStart + batchSize, pointsWithResp.length)
      for (i <- batchStart until batchEnd) {
        var resp = 0.0
        for (j <- pointsWithResp.indices if i != j)
          resp += proximityFunction(pointsWithResp(i).point, pointsWithResp(j).point, epsilon)
        pointsWithResp(i).responsibility = resp
      }
    }

    for (_ <- 0 until maxIterations) {
      var spatialIndex: STRtree = null
      val localityThreshold = 4.0 * epsilon

      if (useLocalityOptimization) {
        spatialIndex = new STRtree()
        pointsWithResp.foreach { p =>
          val env = new Envelope(
            p.point._1 - localityThreshold, p.point._1 + localityThreshold,
            p.point._2 - localityThreshold, p.point._2 + localityThreshold
          )
          spatialIndex.insert(env, p)
        }
        spatialIndex.build()
      }

      val iterBatchSize = math.min(DEFAULT_BATCH_SIZE, dataset.length)
      for (batchStart <- 0 until dataset.length by iterBatchSize) {
        val batchEnd = math.min(batchStart + iterBatchSize, dataset.length)
        for (idx <- batchStart until batchEnd if !randomIndices.contains(idx)) {
          val (fIdx, gIdx) = dataset(idx)
          val candidate = extractPoint(geometries(gIdx))
          var newResp = 0.0

          if (useLocalityOptimization && spatialIndex != null) {
            val env = new Envelope(
              candidate._1 - localityThreshold, candidate._1 + localityThreshold,
              candidate._2 - localityThreshold, candidate._2 + localityThreshold
            )
            val neighbors = spatialIndex.query(env).asInstanceOf[java.util.List[PointWithResponsibility]]
            for (n <- neighbors.asScala) {
              val prox = proximityFunction(candidate, n.point, epsilon)
              newResp += prox
              n.responsibility += prox
            }
          } else {
            for (p <- pointsWithResp) {
              val prox = proximityFunction(candidate, p.point, epsilon)
              newResp += prox
              p.responsibility += prox
            }
          }

          pointsWithResp.append(PointWithResponsibility(candidate, newResp, fIdx, gIdx))

          val maxRespIdx = pointsWithResp.indices.maxBy(i => pointsWithResp(i).responsibility)
          val toRemove = pointsWithResp(maxRespIdx)

          if (useLocalityOptimization && spatialIndex != null) {
            val env = new Envelope(
              toRemove.point._1 - localityThreshold, toRemove.point._1 + localityThreshold,
              toRemove.point._2 - localityThreshold, toRemove.point._2 + localityThreshold
            )
            val neighbors = spatialIndex.query(env).asInstanceOf[java.util.List[PointWithResponsibility]]
            for (n <- neighbors.asScala if n != toRemove)
              n.responsibility -= proximityFunction(toRemove.point, n.point, epsilon)
          } else {
            for (p <- pointsWithResp if p != toRemove)
              p.responsibility -= proximityFunction(toRemove.point, p.point, epsilon)
          }

          pointsWithResp.remove(maxRespIdx)
        }

        if (batchStart + iterBatchSize < dataset.length)
          System.gc()
      }
    }

    val resultIndices = pointsWithResp.flatMap { point =>
      val fIdx = point.featureIndex
      val gIdx = point.geometryIndex
      if (
        fIdx >= 0 && fIdx < features.length &&
          gIdx >= 0 && gIdx < geometries.length &&
          features(fIdx) != null &&
          geometries(gIdx) != null
      ) Some(fIdx)
      else None
    }.toArray

    println(s"[generateVASSample] Returning ${resultIndices.length} valid indices")
    resultIndices
  }
}
