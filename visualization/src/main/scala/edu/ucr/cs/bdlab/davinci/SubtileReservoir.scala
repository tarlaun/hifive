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
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import scala.collection.mutable.ArrayBuffer

/**
 * A reservoir of features and geometries for a specific tile subdivision ("sub-tile").
 *
 * @param features          a mutable buffer of features (with non-spatial attributes) stored in this reservoir
 * @param geometries        a mutable buffer of LiteGeometry objects corresponding to the features
 * @param numPoints         total number of coordinate points across all geometries in this reservoir
 * @param totalFeaturesSeen total number of features that have been sampled and considered for this reservoir
 */
@DefaultSerializer(classOf[SubtileReservoirSerializer])
class SubtileReservoir(
                        var features: ArrayBuffer[IFeature],
                        var geometries: ArrayBuffer[LiteGeometry],
                        var numPoints: Int,
                        var totalFeaturesSeen: Long
                      ) extends Serializable {

  /**
   * Appends a new feature and its geometry to this reservoir, updating counters accordingly.
   *
   * @param feature   the feature (attribute data) to add
   * @param geometry  the geometry corresponding to that feature
   */
  def add(feature: IFeature, geometry: LiteGeometry): Unit = {
    features.append(feature)
    geometries.append(geometry)
    numPoints += geometry.numPoints
    totalFeaturesSeen += 1
  }

  /**
   * Reservoir-size-limited append using reservoir sampling logic.
   * If `capacity` is not yet reached, appends directly. If it is reached,
   * potentially replaces an existing element at random.
   *
   * @param feature   the feature (attribute data) to add
   * @param geometry  the geometry corresponding to that feature
   * @param capacity  maximum allowed size of the reservoir
   */
  def addWithReservoirSampling(feature: IFeature, geometry: LiteGeometry, capacity: Int): Unit = {
    totalFeaturesSeen += 1
    if (features.size < capacity) {
      features.append(feature)
      geometries.append(geometry)
      numPoints += geometry.numPoints
    } else {
      // Standard reservoir sampling: pick a random index in [1..totalFeaturesSeen]
      val r = (scala.util.Random.nextDouble() * totalFeaturesSeen).toLong + 1L
      if (r <= capacity) {
        // Replace a random existing item
        val replaceIdx = (r - 1).toInt
        // Adjust numPoints count
        numPoints -= geometries(replaceIdx).numPoints
        // Do the replacement
        features(replaceIdx) = feature
        geometries(replaceIdx) = geometry
        numPoints += geometry.numPoints
      }
    }
  }

  /**
   * Returns a (deep) copy of this reservoir's data. The new instance
   * will contain cloned lists of features and geometries.
   */
  def copyReservoir(): SubtileReservoir = {
    val newFeatures   = features.map(_.copy().asInstanceOf[IFeature])
    val newGeometries = geometries.map(_.copy())
    new SubtileReservoir(
      features          = newFeatures,
      geometries        = newGeometries,
      numPoints         = this.numPoints,
      totalFeaturesSeen = this.totalFeaturesSeen
    )
  }

  /**
   * Clears all contents of this reservoir.
   */
  def clear(): Unit = {
    features.clear()
    geometries.clear()
    numPoints = 0
    totalFeaturesSeen = 0L
  }
}
