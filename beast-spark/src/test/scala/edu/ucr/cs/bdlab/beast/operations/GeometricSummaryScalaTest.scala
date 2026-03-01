/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.operations

import java.io.File
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.synopses.Summary
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.indexing.GridPartitioner
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometricSummaryScalaTest extends FunSuite with ScalaSparkTest {

  test("Summary with column names") {
    val inputfile = makeResourceCopy("/test.partitions")
    val opts = new BeastOptions().set("iformat", "wkt(Geometry)")
      .set("skipheader", true)
      .set("separator", "\t")
    val summary = GeometricSummary.run(opts, Array(inputfile.getPath), null, sparkContext).asInstanceOf[Summary]
    assert(summary.numFeatures == 44)
  }

  test("Summary with input format and no output format") {
    val inputfile = makeResourceCopy("/test.partitions")
    val opts = new BeastOptions().set("iformat", "wkt(Geometry)")
        .set("skipheader", true)
        .set("separator", "\t")
        .set("oformat", "invalid")
    val summary = GeometricSummary.run(opts, Array(inputfile.getPath), null, sparkContext).asInstanceOf[Summary]
    assert(summary.numFeatures == 44)
  }

  test("Should not read the files if the input is indexed") {
    val inputDir: File = new File(scratchDir, "parks_index")
    makeResourceCopy("/parks_index", inputDir)
    // Add fake (but corrupted) files
    for (i <- 0 to 3) {
      val filename = new File(inputDir, f"part-$i%05d.csv")
      copyResource("/sjoinr.wkt", filename)
    }
    // Now read the file
    val data: SpatialRDD = sparkContext.spatialFile(inputDir.getPath, "wkt")
    val summary = data.summary
    // Summary information should be read from the index not the file
    assertResult(4000)(summary.size)
    assertResult(40)(summary.numFeatures)
  }

  test("Summary should cache the results") {
    val inputfile = new File(scratchDir, "test.partitions")
    copyResource("/test.partitions", inputfile)
    val data: SpatialRDD = sparkContext.spatialFile(inputfile.getPath,
      Seq("skipheader" -> true, "iformat" -> "wkt(Geometry)"))

    val summary1 = data.summary
    // Now delete the file and try again. It should still produce the answer
    inputfile.delete()
    val summary2 = data.summary
    assertResult(44)(summary2.numFeatures)
    var errorRaised = false
    try {
      data.collect()
    } catch {
      case _: Throwable => errorRaised = true
    }
    assertResult(true)(errorRaised)
  }
}
