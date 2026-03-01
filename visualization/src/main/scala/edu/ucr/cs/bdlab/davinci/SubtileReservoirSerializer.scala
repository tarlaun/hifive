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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.Serializer
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import scala.collection.mutable.ArrayBuffer

/**
 * Kryo serializer for [[SubtileReservoir]] to ensure
 * data remains intact during Spark shuffle stages.
 */
class SubtileReservoirSerializer extends Serializer[SubtileReservoir] {

  /**
   * Writes a SubtileReservoir object to the Kryo output stream.
   */
  override def write(kryo: Kryo, output: Output, reservoir: SubtileReservoir): Unit = {
    // 1. Write number of features
    output.writeInt(reservoir.features.size)
    // 2. Write each feature via Kryo
    for (f <- reservoir.features) {
      kryo.writeClassAndObject(output, f)
    }

    // 3. Write number of geometries
    output.writeInt(reservoir.geometries.size)
    // 4. Write each geometry
    for (g <- reservoir.geometries) {
      kryo.writeClassAndObject(output, g)
    }

    // 5. Write numeric fields
    output.writeInt(reservoir.numPoints)
    output.writeLong(reservoir.totalFeaturesSeen)
  }

  /**
   * Reads a SubtileReservoir object from the Kryo input stream.
   */
  override def read(kryo: Kryo, input: Input, `type`: Class[SubtileReservoir]): SubtileReservoir = {
    // 1. Read number of features
    val numFeatures = input.readInt()
    val features = new ArrayBuffer[IFeature](numFeatures)
    // 2. Read each feature
    for (_ <- 0 until numFeatures) {
      val f = kryo.readClassAndObject(input).asInstanceOf[IFeature]
      features.append(f)
    }

    // 3. Read number of geometries
    val numGeometries = input.readInt()
    val geometries = new ArrayBuffer[LiteGeometry](numGeometries)
    // 4. Read each geometry
    for (_ <- 0 until numGeometries) {
      val g = kryo.readClassAndObject(input).asInstanceOf[LiteGeometry]
      geometries.append(g)
    }

    // 5. Read numeric fields
    val numPoints         = input.readInt()
    val totalFeaturesSeen = input.readLong()

    // 6. Construct a new SubtileReservoir
    new SubtileReservoir(features, geometries, numPoints, totalFeaturesSeen)
  }
}
