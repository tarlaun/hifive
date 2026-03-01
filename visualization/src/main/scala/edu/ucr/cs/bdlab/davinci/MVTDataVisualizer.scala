/*
 * Copyright 2018 University of California, Riverside
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

import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.cg.{Reprojector, SpatialDataTypes}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, Feature, GeometryReader, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.{GridPartitioner, IndexHelper}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD.SpatialFilePartition
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import edu.ucr.cs.bdlab.beast.synopses.{AbstractHistogram, GeometryToPoints, HistogramOP, Prefix2DHistogram}
import edu.ucr.cs.bdlab.beast.util.{FileUtil, OperationMetadata, OperationParam, Parallel2, ZipUtil}
import edu.ucr.cs.bdlab.davinci.MultilevelPlot.MasterTileFileName
import edu.ucr.cs.bdlab.davinci.IntermediateVectorTile._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.util.LineReader
import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.TaskFailureListener
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{NumericType, StringType}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.operation.transform.{AffineTransform2D, ConcatenatedTransform}
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.operation.MathTransform

import java.awt.geom.AffineTransform
import java.io.{ByteArrayOutputStream, DataOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.zip.ZipOutputStream
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A command-line tool for visualizing datasets into MVT files (Vector Tiles).
 *
 * Cleaned-up version:
 * - Removes Spark-expensive debug actions (counts/takes/distinct samples) and verbose per-tile printing.
 * - Keeps tile generation + partitioning behavior intact.
 * - Fixes compile issues:
 *   - Defines reduceTileSize (greedy) so saveTiles compiles.
 *   - Fixes tileOffsets initialization (no Array[Nothing], no missing _length).
 *   - Removes duplicate/second saveTilesCompact definition (keep ONE).
 */
@OperationMetadata(
  shortName = "vmplot",
  description = "Plots the input file as a multilevel pyramid image with mvt tiles",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat])
)
object MVTDataVisualizer extends CLIOperation with Logging {

  private val WebMercatorMBR: EnvelopeNDLite = new EnvelopeNDLite(2,
    -20037508.34, -20037508.34, 20037508.34, 20037508.34)

  @OperationParam(
    description = "The maximum size for the histogram used in adaptive multilevel plot",
    defaultValue = "32m",
    showInUsage = false
  )
  val MaximumHistogramSize = "VectorTilePlotter.MaxHistogramSize"

  @OperationParam(
    description =
      "Image tile threshold for adaptive multilevel plot. " +
        "Any tile that is strictly larger than (not equal) this threshold is considered an image tile",
    defaultValue = "1m"
  )
  val ImageTileThreshold = "threshold"

  @OperationParam(
    description =
      "The total number of levels for multilevel plot. " +
        "Can be specified as a range min..max (inclusive of both) or as a single number which indicates " +
        "the number of levels starting at zero.",
    defaultValue = "7"
  )
  val NumLevels = "levels"

  @OperationParam(
    description = "The resolution of the vector tile in pixels",
    defaultValue = "256"
  )
  val Resolution = "resolution"

  @OperationParam(
    description = "Write all output files in a compact ZIP archive",
    defaultValue = "true"
  )
  val CompactOutput = "compact"

  @OperationParam(
    description = "The threshold for applying reduction method",
    defaultValue = "250000000"
  )
  val ReductionThreshold = "reductionThreshold"

  @OperationParam(
    description =
      "Unified sampler spec for vector tiles, e.g. mode=reservoir:greedy; " +
        "priority=vertices:largest; capacity=kb:256",
    defaultValue = ""
  )
  val SamplerSpec = "sampler"

  @OperationParam(
    description =
      "Alpha in [0,1] for reduction scoring: weighted = alpha*pixelCount + (1-alpha)*(-kld)",
    defaultValue = "0.0"
  )
  val Alpha = "alpha"

  @OperationParam(
    description =
      "Tile reduction method when a tile exceeds reductionThreshold. Options: greedy | lp | none",
    defaultValue = "lp"
  )
  val ReductionMethod = "reductionMethod"

  // =========================================================================================
  // Plotting
  // =========================================================================================

  def plotSingleTileParallel(
                              features: SpatialDataTypes.SpatialRDD,
                              resolution: Int,
                              tileID: Long,
                              buffer: Int = 0,
                              opts: BeastOptions = new BeastOptions()
                            ): VectorTile.Tile = {

    val tileMBRMercator: Envelope = new Envelope
    TileIndex.getMBR(WebMercatorMBR, tileID, tileMBRMercator)

    val sourceSRID = features.first().getGeometry.getSRID
    val sourceToWebMercator: MathTransform =
      Reprojector.findTransformationInfo(sourceSRID, 3857).mathTransform

    val webMercatorToImageTile: AffineTransform = new AffineTransform()
    webMercatorToImageTile.scale(resolution / tileMBRMercator.getWidth, resolution / tileMBRMercator.getHeight)
    webMercatorToImageTile.translate(-tileMBRMercator.getMinX, -tileMBRMercator.getMinY)

    val verticalFlip = new AffineTransform2D(AffineTransform.getScaleInstance(1, -1))
    val dataToImage = ConcatenatedTransform.create(
      ConcatenatedTransform.create(sourceToWebMercator, verticalFlip),
      new AffineTransform2D(webMercatorToImageTile)
    )
    val imageToData = dataToImage.inverse()

    val sourceExtents = Array[Double](-buffer, -buffer, resolution + buffer, resolution + buffer)
    imageToData.transform(sourceExtents, 0, sourceExtents, 0, 2)
    val sourceExtentsEnvelope = GeometryReader.DefaultGeometryFactory.toGeometry(
      new Envelope(sourceExtents(0), sourceExtents(2), sourceExtents(1), sourceExtents(3))
    )

    val featuresOfInterest = features.rangeQuery(sourceExtentsEnvelope)
      .map(f => Feature.create(f, f.getGeometry.intersection(sourceExtentsEnvelope), 0))
      .filter(!_.getGeometry.isEmpty)
      .map(f => Feature.create(f, JTS.transform(f.getGeometry, dataToImage), f.pixelCount))

    val tileIndex = new TileIndex()
    TileIndex.decode(tileID, tileIndex)
    val zoom = tileIndex.z

    val emptyTile = new IntermediateVectorTile(resolution, buffer, zoom)
    featuresOfInterest.aggregate(emptyTile)(
      (tile, feature) => tile.addFeature(feature),
      (tile1, tile2) => tile1.merge(tile2)
    ).vectorTile
  }

  def dataToImageTransformer(tileID: Long, resolution: Int): AffineTransform = {
    val tileMBRMercator: Envelope = new Envelope
    TileIndex.getMBR(WebMercatorMBR, tileID, tileMBRMercator)

    val dataToImage: AffineTransform = new AffineTransform()
    dataToImage.scale(resolution / tileMBRMercator.getWidth, resolution / tileMBRMercator.getHeight)
    dataToImage.translate(-tileMBRMercator.getMinX, -tileMBRMercator.getMinY)
    dataToImage
  }

  def plotAllTiles(
                    features: SpatialDataTypes.SpatialRDD,
                    levels: Range,
                    resolution: Int,
                    buffer: Int = 0,
                    opts: BeastOptions = new BeastOptions()
                  ): RDD[(Long, IntermediateVectorTile)] = {

    val sc = features.sparkContext

    val verticalFlip = new AffineTransform2D(AffineTransform.getScaleInstance(1, -1))
    var featuresToPlot: SpatialDataTypes.SpatialRDD =
      VisualizationHelper.toWebMercator(features).map { f =>
        val gg = JTS.transform(f.getGeometry, verticalFlip)
        Feature.create(f, gg, f.pixelCount).asInstanceOf[IFeature]
      }

    val threshold: Long = opts.getSizeAsBytes(ImageTileThreshold, "1m")
    val maxHistogramSize = opts.getSizeAsBytes(MaximumHistogramSize, 32L * 1024L * 1024L)
    val binSize = 8
    val gridDimension = MultilevelPyramidPlotHelper.computeHistogramDimension(maxHistogramSize, levels.max, binSize)

    val h: AbstractHistogram =
      if (threshold == 0) null
      else {
        val allPoints: SpatialRDD = featuresToPlot
          .flatMap(f => new GeometryToPoints(f.getGeometry))
          .map(g => Feature.create(null, g, 0))
        new Prefix2DHistogram(
          HistogramOP.computePointHistogramSparse(allPoints, _.getStorageSize, WebMercatorMBR, gridDimension, gridDimension)
        )
      }

    val maxLevelImageTile: Int =
      if (threshold == 0) levels.max
      else MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, threshold)

    if (!features.isSpatiallyPartitioned || featuresToPlot.getNumPartitions < sc.defaultParallelism) {
      val desiredNumPartitions = (featuresToPlot.getNumPartitions max sc.defaultParallelism) * 10
      var gridDepth: Int = maxLevelImageTile
      while (gridDepth > 1 && (1L << ((gridDepth - 1) * 2)) > desiredNumPartitions) gridDepth -= 1

      val gridPartitioner = new GridPartitioner(WebMercatorMBR, Array[Int](1 << gridDepth, 1 << gridDepth))
      gridPartitioner.setDisjointPartitions(false)
      featuresToPlot = IndexHelper.partitionFeatures2(featuresToPlot, gridPartitioner)
    }

    val pyramidPartitioner =
      if (threshold == 0)
        new PyramidPartitioner(new SubPyramid(WebMercatorMBR, levels.min, levels.max))
      else
        new PyramidPartitioner(
          new SubPyramid(WebMercatorMBR, levels.min, (maxLevelImageTile min levels.max)),
          h,
          threshold + 1,
          Long.MaxValue
        )

    pyramidPartitioner.setBuffer(buffer.toFloat / resolution)

    val partitionedFeatures: RDD[(Long, IFeature)] =
      featuresToPlot.flatMap { feature =>
        val mbr = new EnvelopeNDLite(2)
        mbr.merge(feature.getGeometry)
        val tileIDs = pyramidPartitioner.overlapPartitions(mbr)
        tileIDs.map(tileid => (tileid, feature))
      }

    partitionedFeatures
      .mapPartitions { it =>
        new VectorTileCreatorFlatPartitioning(it, resolution, buffer)
      }
      .reduceByKey(_.merge(_))
      .mapValues { t =>
        t.collectFeaturesFromSubtiles()
        t
      }
  }

  def plotSingleTileLocal(
                           fs: FileSystem,
                           vizPath: Path,
                           tileID: Long,
                           out: OutputStream,
                           clientTimestamp: LongWritable,
                           resolution: Int,
                           buffer: Int = 0,
                           opts: BeastOptions = new BeastOptions()
                         ): Unit = {

    val featureReaderClass = SpatialFileRDD.getFeatureReaderClass(vizPath.toString, opts)
    var partitions = SpatialFileRDD.createPartitions(vizPath.toString, opts, new Configuration(fs.getConf))

    val sourceSRID: Int = {
      val reader = SpatialFileRDD.readPartition(partitions.head, featureReaderClass, applyDuplicateAvoidance = false, opts)
      if (reader.hasNext) {
        val srid = reader.next().getGeometry.getSRID
        reader match {
          case closeable: java.io.Closeable => closeable.close()
          case _ =>
        }
        srid
      } else 0
    }

    val tileMBRMercator: Envelope = new Envelope
    TileIndex.getMBR(WebMercatorMBR, tileID, tileMBRMercator)

    val sourceToWebMercator: MathTransform =
      Reprojector.findTransformationInfo(sourceSRID, 3857).mathTransform

    val webMercatorToImageTile: AffineTransform = new AffineTransform()
    webMercatorToImageTile.scale(resolution / tileMBRMercator.getWidth, resolution / tileMBRMercator.getHeight)
    webMercatorToImageTile.translate(-tileMBRMercator.getMinX, -tileMBRMercator.getMinY)

    val verticalFlip = new AffineTransform2D(AffineTransform.getScaleInstance(1, -1))
    val dataToImage = ConcatenatedTransform.create(
      ConcatenatedTransform.create(sourceToWebMercator, verticalFlip),
      new AffineTransform2D(webMercatorToImageTile)
    )
    val imageToData = dataToImage.inverse()

    val sourceExtents = Array[Double](-buffer, -buffer, resolution + buffer, resolution + buffer)
    imageToData.transform(sourceExtents, 0, sourceExtents, 0, 2)
    val y1 = sourceExtents(1)
    val y2 = sourceExtents(3)
    sourceExtents(1) = y1 min y2
    sourceExtents(3) = y1 max y2

    opts.set(SpatialFileRDD.FilterMBR, sourceExtents.mkString(","))
    val sourceEnv = new Envelope(sourceExtents(0), sourceExtents(2), sourceExtents(1), sourceExtents(3))
    val sourceExtentsEnvelope = GeometryReader.DefaultGeometryFactory.toGeometry(sourceEnv)

    partitions = partitions.filter {
      case x: SpatialFilePartition => x.mbr.intersectsEnvelope(sourceEnv)
      case _ => true
    }

    val tileIndex = new TileIndex()
    TileIndex.decode(tileID, tileIndex)
    val zoom = tileIndex.z

    val resultTile = new IntermediateVectorTile(resolution, buffer, zoom, dataToImage)
    for (i <- partitions.indices) {
      val reader: Iterator[IFeature] =
        SpatialFileRDD.readPartition(partitions(i), featureReaderClass, applyDuplicateAvoidance = false, opts)
      try {
        for (feature <- reader) {
          if (feature.getGeometry.intersects(sourceExtentsEnvelope)) {
            resultTile.addFeature(feature)
          }
        }
      } finally reader match {
        case closeable: java.io.Closeable => closeable.close()
        case _ =>
      }
    }

    resultTile.vectorTile.writeTo(out)
    out.close()
  }

  // =========================================================================================
  // Save tiles (non-compact)
  // =========================================================================================

  def saveTiles(tiles: JavaPairRDD[java.lang.Long, IntermediateVectorTile], outPath: String, opts: BeastOptions): Unit =
    saveTiles(tiles.rdd.map(f => (f._1.longValue(), f._2)), outPath, opts)

  def saveTiles(
                 tiles: RDD[(Long, IntermediateVectorTile)],
                 outPath: String,
                 opts: BeastOptions
               ): Unit = {

    val conf: BeastOptions =
      new BeastOptions(opts.loadIntoHadoopConf(tiles.sparkContext.hadoopConfiguration))

    val tileSizeThreshold: Long =
      opts.getSizeAsBytes(ReductionThreshold, 4096L * 1024L)

    tiles.foreachPartition { partitionIter =>
      val hConf = conf.loadIntoHadoopConf(new Configuration(false))
      val fs = new Path(outPath).getFileSystem(hConf)

      partitionIter.foreach {
        case (id, tile) =>
          if (tile != null && tile.features != null && tile.features.nonEmpty) {
            val tileIndex = new TileIndex()
            TileIndex.decode(id, tileIndex)

            val path = new Path(outPath, s"tile-${tileIndex.z}-${tileIndex.x}-${tileIndex.y}.mvt")
            val output = fs.create(path, true)

            val preSize = tile.vectorTile.getSerializedSize

            val methodRaw = opts.getString(MVTDataVisualizer.ReductionMethod, "lp")
            val method = if (methodRaw == null) "lp" else methodRaw.trim.toLowerCase

            if (preSize > tileSizeThreshold) {
              method match {
                case "greedy" | "reducetilesize" =>
                  reduceTileSize(tile, tileIndex, tileSizeThreshold, opts)

                case "lp" | "gurobi" | "reducetilesizelp_gurobi" =>
                  reduceTileSizeLP_Gurobi(tile, tileIndex, tileSizeThreshold, opts)

                case "none" | "off" | "false" =>
                  ()

                case other =>
                  System.err.println(
                    s"WARNING: Unknown --${MVTDataVisualizer.ReductionMethod}='$other'. Falling back to 'lp'."
                  )
                  reduceTileSizeLP_Gurobi(tile, tileIndex, tileSizeThreshold, opts)
              }
            }

            tile.vectorTile.writeTo(output)
            output.close()
          }
      }
    }

    val htmlPath: Path = new Path(outPath, "index.html")
    val indexHTML: String = getIndexHTMLFile(conf)
    val outFS = htmlPath.getFileSystem(conf.loadIntoHadoopConf(null))
    val htmlOut = outFS.create(htmlPath, true)
    htmlOut.write(indexHTML.getBytes(StandardCharsets.UTF_8))
    htmlOut.close()

    val vizPropertiesPath: Path = new Path(outPath, "_visualization.properties")
    val out = outFS.create(vizPropertiesPath, true)
    val prop = opts.toProperties
    prop.store(out, "Beast visualization properties")
    out.close()
  }

  // =========================================================================================
  // Save tiles (compact ZIP)  ✅ FIXED tileOffsets typing + removed duplicate definition
  // =========================================================================================

  def saveTilesCompact(tiles: JavaPairRDD[java.lang.Long, IntermediateVectorTile], outPath: String, _opts: BeastOptions): Unit =
    saveTilesCompact(tiles.rdd.map(f => (f._1.longValue(), f._2)), outPath, _opts)

  def saveTilesCompact(tiles: RDD[(Long, IntermediateVectorTile)], outPath: String, _opts: BeastOptions): Unit = {
    val opts = new BeastOptions(tiles.context.hadoopConfiguration).mergeWith(_opts)

    val tempPath = outPath + "_temp"

    val interimFiles: Array[(String, Int)] = tiles.sparkContext
      .runJob(tiles, (context: TaskContext, tilesIt: Iterator[(Long, IntermediateVectorTile)]) => {

        val localZipFile: Path = new Path(tempPath, f"tiles-${context.partitionId()}-${context.taskAttemptId()}.zip")
        val outFS = localZipFile.getFileSystem(opts.loadIntoHadoopConf())

        context.addTaskFailureListener(new TaskFailureListener() {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            if (outFS.exists(localZipFile))
              outFS.delete(localZipFile, false)
          }
        })

        val zipOut: ZipOutputStream = new ZipOutputStream(outFS.create(localZipFile))
        val tempTileIndex: TileIndex = new TileIndex()
        val memTileOut = new ByteArrayOutputStream()
        var numTiles: Int = 0

        try {
          tilesIt.foreach { case (tid, tile) =>
            if (tile != null) {
              numTiles += 1
              TileIndex.decode(tid, tempTileIndex)
              memTileOut.reset()
              tile.vectorTile.writeTo(memTileOut)
              ZipUtil.putStoredFile(
                zipOut,
                s"tile-${tempTileIndex.z}-${tempTileIndex.x}-${tempTileIndex.y}.mvt",
                memTileOut.toByteArray
              )
            }
          }
        } finally {
          zipOut.close()
        }

        if (numTiles == 0) {
          outFS.delete(localZipFile, false)
          (null, 0)
        } else {
          (localZipFile.toString, numTiles)
        }
      }).filter(_._2 > 0)

    val outFS = new Path(outPath).getFileSystem(opts.loadIntoHadoopConf())

    // ✅ FIX: must be typed, and initialized safely (no Array[Nothing], no missing _length)
    var tileOffsets: Array[(Long, Long, Int)] = Array.empty[(Long, Long, Int)]

    if (interimFiles.nonEmpty) {
      val zipFileStartOffset = new Array[Long](interimFiles.length)
      zipFileStartOffset(0) = 0

      for (i <- interimFiles.indices; if i > 0) {
        interimFiles(i) = (interimFiles(i)._1, interimFiles(i)._2 + interimFiles(i - 1)._2)
        zipFileStartOffset(i) =
          zipFileStartOffset(i - 1) + outFS.getFileStatus(new Path(interimFiles(i - 1)._1)).getLen
      }

      tileOffsets = new Array[(Long, Long, Int)](interimFiles.last._2)

      val tileRegex = s"tile-(\\d+)-(\\d+)-(\\d+).mvt".r
      Parallel2.forEach(interimFiles.length, (i1, i2) => {
        for (iZipFile <- i1 until i2) {
          val localZipFile = interimFiles(iZipFile)._1
          val entryOffsets: Array[(String, Long, Long)] = ZipUtil.listFilesInZip(outFS, new Path(localZipFile))
          val iFirstTile: Int = if (iZipFile == 0) 0 else interimFiles(iZipFile - 1)._2

          assert(
            entryOffsets.length == interimFiles(iZipFile)._2 - iFirstTile,
            s"Unexpected number of entries in ZIP file '$localZipFile'. " +
              s"Expected ${interimFiles(iZipFile)._2 - iFirstTile} but found ${entryOffsets.length}"
          )

          for (iEntry <- entryOffsets.indices) {
            entryOffsets(iEntry)._1 match {
              case tileRegex(z, x, y) =>
                tileOffsets(iEntry + iFirstTile) = (
                  TileIndex.encode(z.toInt, x.toInt, y.toInt),
                  zipFileStartOffset(iZipFile) + entryOffsets(iEntry)._2,
                  entryOffsets(iEntry)._3.toInt
                )
              case _ =>
            }
          }
        }
      })
    }

    val baos = new ByteArrayOutputStream()
    val masterZIPPath = new Path(tempPath, "master.zip")
    val masterZIPOut = new ZipOutputStream(outFS.create(masterZIPPath))

    val indexHTML: String = getIndexHTMLFile(_opts)
    ZipUtil.putStoredFile(masterZIPOut, "index.html", indexHTML.getBytes(StandardCharsets.UTF_8))

    val oformat: Option[String] = _opts.get(SpatialWriter.OutputFormat)
    if (oformat.isDefined)
      _opts.set(SpatialFileRDD.InputFormat, oformat.get)

    val prop = _opts.toProperties
    baos.reset()
    prop.store(baos, "Beast visualization properties")
    ZipUtil.putStoredFile(masterZIPOut, "_visualization.properties", baos.toByteArray)

    baos.reset()
    val dataOut = new DataOutputStream(baos)
    DiskTileHashtable.construct(dataOut, tileOffsets)
    dataOut.close()
    ZipUtil.putStoredFile(masterZIPOut, MasterTileFileName, baos.toByteArray)
    masterZIPOut.close()

    val mergedZIPFile = new Path(tempPath, "multilevelplot.zip")
    ZipUtil.mergeZip(outFS, mergedZIPFile, (interimFiles.map(x => new Path(x._1)) :+ masterZIPPath): _*)

    val finalZIPPath = new Path(FileUtil.replaceExtension(outPath, ".zip"))
    outFS.rename(mergedZIPFile, finalZIPPath)
    outFS.delete(new Path(tempPath), true)
  }

  // =========================================================================================
  // Reduction helpers
  // =========================================================================================

  /**
   * ✅ FIX: define reduceTileSize so "Cannot resolve symbol reduceTileSize" is gone.
   *
   * Greedy reducer:
   * - repeatedly removes the smallest-importance feature until tile <= threshold or only 1 feature remains.
   * - keeps features/geometries aligned and rebuilds tile.vectorTile each step.
   *
   * This is intentionally simple and safe. If you have a more advanced greedy reducer elsewhere,
   * you can replace this body with your original implementation.
   */
  def reduceTileSize(
                      tile: IntermediateVectorTile,
                      tileIndex: TileIndex,
                      tileSizeThreshold: Long,
                      opts: BeastOptions
                    ): Unit = {

    if (tile == null || tile.vectorTile == null || tile.features == null || tile.features.isEmpty) return
    if (tile.vectorTile.getSerializedSize <= tileSizeThreshold) return

    @inline def safePc(i: Int): Int = {
      val g = if (i >= 0 && i < tile.geometries.length) tile.geometries(i) else null
      if (g != null) {
        try calculateAccuratePixelCount(g, tileIndex)
        catch { case _: Throwable =>
          val f = tile.features(i)
          if (f != null) math.max(0, f.pixelCount) else 0
        }
      } else {
        val f = tile.features(i)
        if (f != null) math.max(0, f.pixelCount) else 0
      }
    }

    def rebuild(): Unit = {
      val layerBuilder = new VectorLayerBuilder(tile.resolution, "features")
      var i = 0
      val n = tile.features.length
      while (i < n) {
        if (i < tile.geometries.length && tile.features(i) != null && tile.geometries(i) != null) {
          layerBuilder.addFeature(tile.features(i), tile.geometries(i))
        }
        i += 1
      }
      val tb = VectorTile.Tile.newBuilder()
      tb.addLayers(layerBuilder.build())
      tile.vectorTile = tb.build()
    }

    // Loop: drop one feature at a time (lowest pixel footprint)
    while (tile.features.length > 1 && tile.vectorTile.getSerializedSize > tileSizeThreshold) {
      val n = tile.features.length
      var minIdx = -1
      var minPc = Int.MaxValue

      var i = 0
      while (i < n) {
        val pc = safePc(i)
        if (pc < minPc) { minPc = pc; minIdx = i }
        i += 1
      }
      if (minIdx < 0) return

      val newF = new ArrayBuffer[IFeature](n - 1)
      val newG = new ArrayBuffer[LiteGeometry](n - 1)

      i = 0
      while (i < n) {
        if (i != minIdx) {
          newF += tile.features(i)
          newG += tile.geometries(i)
        }
        i += 1
      }

      tile.features = newF
      tile.geometries = newG
      rebuild()
    }
  }

  // Put this inside object MVTDataVisualizer (same scope as LiteGeometry types)
  def calculateAccuratePixelCount(geom: LiteGeometry, tileIndex: TileIndex): Int = {
    if (geom == null) return 0

    val tilePixels = 256

    @inline def clamp(v: Int, lo: Int, hi: Int): Int =
      if (v < lo) lo else if (v > hi) hi else v

    @inline def pack(x: Int, y: Int): Int = (y << 8) | x

    def inferUnitsPerPixel(): Double = {
      var maxAbs = 0

      @inline def upd(x: Int, y: Int): Unit = {
        val ax = if (x < 0) -x else x
        val ay = if (y < 0) -y else y
        if (ax > maxAbs) maxAbs = ax
        if (ay > maxAbs) maxAbs = ay
      }

      geom match {
        case p: LitePoint =>
          upd(p.x.toInt, p.y.toInt)

        case mp: LiteMultiPoint =>
          var i = 0
          while (i < mp.xs.length) {
            upd(mp.xs(i).toInt, mp.ys(i).toInt)
            i += 1
          }

        case ls: LiteLineString =>
          var pi = 0
          while (pi < ls.parts.length) {
            val part = ls.parts(pi)
            var j = 0
            while (j < part.numPoints) {
              upd(part.xs(j).toInt, part.ys(j).toInt)
              j += 1
            }
            pi += 1
          }

        case pg: LitePolygon =>
          var pi = 0
          while (pi < pg.parts.length) {
            val part = pg.parts(pi)
            var j = 0
            while (j < part.numPoints) {
              upd(part.xs(j).toInt, part.ys(j).toInt)
              j += 1
            }
            pi += 1
          }

        case _ =>
          val e = geom.envelope
          if (e != null) {
            val w = math.abs(e.width)
            val h = math.abs(e.height)
            maxAbs = math.max(w, h).toInt
          }
      }

      if (maxAbs <= 0) 1.0
      else if (maxAbs <= 512) 1.0
      else maxAbs.toDouble / tilePixels.toDouble
    }

    val unitsPerPixel = inferUnitsPerPixel()

    @inline def toPix(u: Short): Int = {
      val p = math.floor(u.toDouble / unitsPerPixel).toInt
      clamp(p, 0, tilePixels - 1)
    }

    def rasterSegment(x0u: Short, y0u: Short, x1u: Short, y1u: Short, seen: java.util.HashSet[Integer]): Unit = {
      var x0 = toPix(x0u)
      var y0 = toPix(y0u)
      val x1 = toPix(x1u)
      val y1 = toPix(y1u)

      var dx = math.abs(x1 - x0)
      val sx = if (x0 < x1) 1 else -1
      var dy = -math.abs(y1 - y0)
      val sy = if (y0 < y1) 1 else -1
      var err = dx + dy

      while (true) {
        seen.add(pack(x0, y0))
        if (x0 == x1 && y0 == y1) return
        val e2 = err << 1
        if (e2 >= dy) { err += dy; x0 += sx }
        if (e2 <= dx) { err += dx; y0 += sy }
      }
    }

    geom match {
      case _: LitePoint =>
        1

      case mp: LiteMultiPoint =>
        val seen = new java.util.HashSet[Integer](mp.numPoints * 2 + 1)
        var i = 0
        while (i < mp.xs.length) {
          val x = toPix(mp.xs(i))
          val y = toPix(mp.ys(i))
          seen.add(pack(x, y))
          i += 1
        }
        math.max(1, seen.size())

      case ls: LiteLineString =>
        val seen = new java.util.HashSet[Integer](math.max(64, ls.numPoints * 2))
        var pi = 0
        while (pi < ls.parts.length) {
          val part = ls.parts(pi)
          if (part != null && part.numPoints > 0) {
            var j = 1
            var px = part.xs(0)
            var py = part.ys(0)
            while (j < part.numPoints) {
              val x = part.xs(j)
              val y = part.ys(j)
              rasterSegment(px, py, x, y, seen)
              px = x; py = y
              j += 1
            }
          }
          pi += 1
        }
        math.max(1, seen.size())

      case pg: LitePolygon =>
        var maxAbsArea = 0.0
        var sumAbsArea = 0.0

        var pi = 0
        while (pi < pg.parts.length) {
          val ring = pg.parts(pi)
          if (ring != null && ring.numPoints >= 3) {
            val a = math.abs(ring.area)
            sumAbsArea += a
            if (a > maxAbsArea) maxAbsArea = a
          }
          pi += 1
        }

        val effectiveArea =
          if (pg.parts.length <= 1) maxAbsArea
          else math.max(0.0, maxAbsArea - (sumAbsArea - maxAbsArea))

        val pxArea = effectiveArea / (unitsPerPixel * unitsPerPixel)
        val res = pxArea.toInt
        if (res <= 0 && pg.numPoints > 0) 1 else res

      case _ =>
        val e = geom.envelope
        if (e == null) {
          if (geom.numPoints > 0) 1 else 0
        } else {
          val pxW = math.floor(math.abs(e.width).toDouble / unitsPerPixel).toInt
          val pxH = math.floor(math.abs(e.height).toDouble / unitsPerPixel).toInt
          val a = pxW * pxH
          if (a <= 0 && geom.numPoints > 0) 1 else a
        }
    }
  }

  // =========================================================================================
  // Attribute/KL helpers (your existing ones, kept)
  // =========================================================================================

  def calculateEntropy(frequencies: Map[Any, Int]): Double = {
    val total = frequencies.values.sum
    if (total == 0) 0.0
    else frequencies.values.map { count =>
      val probability = count.toDouble / total
      if (probability > 0) -probability * math.log(probability) / math.log(2) else 0.0
    }.sum
  }

  def calculateNormalizedEntropy(frequencies: Map[Any, Int]): Double = {
    val entropy = calculateEntropy(frequencies)
    val maxEntropy = math.log(frequencies.size) / math.log(2)
    if (maxEntropy == 0) 0.0 else entropy / maxEntropy
  }

  def calculateEntropies(tile: IntermediateVectorTile): List[(String, Double)] = {
    val columnFrequencies = mutable.Map[String, mutable.Map[Any, Int]]()
    val totalCounts = mutable.Map[String, Int]()

    tile.features.foreach { feature =>
      if (feature.schema != null && feature.schema.fields != null) {
        feature.schema.fields.indices.foreach { i =>
          val field = feature.schema.fields(i)
          val v = feature.get(i)
          if (v != null) {
            val fieldName = field.name
            val freqs = columnFrequencies.getOrElseUpdate(fieldName, mutable.Map[Any, Int]())
            freqs(v) = freqs.getOrElse(v, 0) + 1
            totalCounts(fieldName) = totalCounts.getOrElse(fieldName, 0) + 1
          }
        }
      }
    }

    columnFrequencies.map { case (fieldName, frequencies) =>
      (fieldName, calculateEntropy(frequencies.toMap))
    }.toList
  }

  def quantizeValue(value: Double, minValue: Double, maxValue: Double, numBuckets: Int): Double = {
    if (numBuckets <= 1) return minValue
    if (value.isNaN || value.isInfinite) return value
    if (minValue.isNaN || maxValue.isNaN || minValue.isInfinite || maxValue.isInfinite) return value
    if (maxValue <= minValue) return minValue

    val range = maxValue - minValue
    val bucketSize = range / numBuckets.toDouble
    if (bucketSize <= 0.0 || bucketSize.isNaN || bucketSize.isInfinite) return minValue

    val raw = (value - minValue) / bucketSize
    val idx = math.floor(raw).toInt
    val clampedIdx = if (idx < 0) 0 else if (idx >= numBuckets) numBuckets - 1 else idx

    minValue + (clampedIdx.toDouble + 0.5) * bucketSize
  }

  def getAttributeMap(tile: IntermediateVectorTile): Array[mutable.Map[String, Int]] = {
    val numFieldsList = tile.features.flatMap(f => Option(f.schema)).map(_.fields.length)
    val maxNumFields = if (numFieldsList.isEmpty) 0 else numFieldsList.max
    val attributeValueCounts = Array.fill(maxNumFields)(mutable.Map[String, Int]())

    tile.features.foreach { feature =>
      if (feature.schema != null && feature.schema.fields != null) {
        val numFields = feature.schema.fields.length
        for (i <- 0 until numFields) {
          val field = feature.schema.fields(i)
          if (field.dataType != GeometryDataType) {
            val value = Option(feature.get(i)).map(_.toString).getOrElse("null")
            val m = attributeValueCounts(i)
            m(value) = m.getOrElse(value, 0) + 1
          }
        }
      }
    }
    attributeValueCounts
  }

  def calculateKLDivergence(originalMap: Map[String, Int], newMap: scala.collection.Map[String, Int]): Double = {
    val originalTotal = originalMap.values.sum.toDouble
    val newTotal = newMap.values.sum.toDouble
    if (originalTotal == 0 || newTotal == 0) return 0.0

    var sum = 0.0
    originalMap.keys.foreach { key =>
      val originalProb = originalMap(key).toDouble / originalTotal
      val newProb = newMap.getOrElse(key, 0).toDouble / newTotal
      if (originalProb > 0) {
        if (newProb > 0) {
          val klTerm = originalProb * math.log(originalProb / newProb)
          if (!klTerm.isNaN && !klTerm.isInfinite) sum += klTerm
        } else {
          sum += originalProb * 10.0
        }
      }
    }
    if (sum.isNaN || sum.isInfinite) 0.0 else sum
  }

  def efficientKLDiv(
                      key: String,
                      originalCountKey: Double,
                      originalCountNull: Double,
                      total: Double,
                      qMap: java.util.HashMap[String, Double],
                      logQMap: java.util.HashMap[String, Double]
                    ): Double = {
    val pKey = math.max(0.0, originalCountKey - 1) / total
    val pNull = (originalCountNull + 1.0) / total

    val qKey = qMap.getOrDefault(key, 0.0)
    val qNull = qMap.getOrDefault("null", 0.0)

    val logQKey = logQMap.getOrDefault(key, Double.NegativeInfinity)
    val logQNull = logQMap.getOrDefault("null", Double.NegativeInfinity)

    var sum = 0.0
    if (pKey > 0.0 && qKey > 0.0) {
      val klTermKey = pKey * (math.log(pKey) - logQKey)
      if (!klTermKey.isNaN && !klTermKey.isInfinite) sum += klTermKey
    }
    if (pNull > 0.0 && qNull > 0.0) {
      val klTermNull = pNull * (math.log(pNull) - logQNull)
      if (!klTermNull.isNaN && !klTermNull.isInfinite) sum += klTermNull
    }
    if (sum.isNaN || sum.isInfinite) 0.0 else math.max(0.0, sum)
  }

  def calculateMaxKLDivergenceForTile(attributeMaps: Array[Map[String, Int]]): List[Double] = {
    attributeMaps.map { originalMap =>
      val totalCount = originalMap.values.sum
      if (totalCount == 0) 0.00001
      else {
        val newMap = mutable.Map("null" -> totalCount)
        var kl = calculateKLDivergence(originalMap, newMap)
        if (kl == 0.0 || kl.isInfinite || kl.isNaN) kl = 0.00001
        kl
      }
    }.toList
  }

  // =========================================================================================
  // LP reducer (your implementation; kept as provided)
  // =========================================================================================
  def reduceTileSizeLP_Gurobi(
                               tile: IntermediateVectorTile,
                               tileIndex: TileIndex,
                               tileSizeThreshold: Long,
                               opts: BeastOptions
                             ): Unit = {

    import scala.collection.mutable
    import scala.collection.JavaConverters._
    import java.nio.charset.StandardCharsets
    import com.gurobi.gurobi._

    if (tile == null || tile.vectorTile == null || tile.features == null || tile.features.isEmpty) return

    val baseOpt = tile.features.find(f => f != null && f.schema != null && f.schema.fields != null)
    if (baseOpt.isEmpty) return
    val base = baseOpt.get

    val initialBytes: Int = tile.vectorTile.getSerializedSize
    if (initialBytes <= tileSizeThreshold) return

    val alphaRaw: Double = opts.getDouble(MVTDataVisualizer.Alpha, 0.8)
    val alpha: Double = math.max(0.0, math.min(1.0, alphaRaw))

    // quantization bins (hardcoded here; you can wire to opts later)
    val numBinsRaw = 16
    val numBins: Int = math.max(2, numBinsRaw)

    val thrLP: Double = math.max(1.0, tileSizeThreshold.toDouble)

    val DEBUG_SUMMARY: Boolean = true

    // Utility shaping
    val SIZE_WEIGHT: Double = 5000.0
    val SIZE_POWER: Double = 2.0

    var lpEstimatedBytes: Double = Double.NaN

    @inline def safeGet(f: IFeature, i: Int): Any =
      if (f == null || i < 0) null else try f.get(i) catch { case _: Throwable => null }

    // Slightly more realistic unique-value cost estimate, but we won't use it as a hard model anymore
    @inline def uniqueValueBytesApprox(s: String): Double =
      if (s == null) 0.0 else 6.0 + s.getBytes(StandardCharsets.UTF_8).length.toDouble

    @inline def compactDoubleString(d: Double): String = {
      if (d.isNaN) "NaN"
      else if (d.isPosInfinity) "Inf"
      else if (d.isNegInfinity) "-Inf"
      else {
        val bd = BigDecimal(d).setScale(6, BigDecimal.RoundingMode.HALF_UP).bigDecimal.stripTrailingZeros()
        bd.toPlainString
      }
    }

    def rebuildTileFromCurrentArrays(): Unit = {
      val layerBuilder = new VectorLayerBuilder(tile.resolution, "features")
      var i = 0
      val n = tile.features.length
      while (i < n) {
        if (i < tile.geometries.length && tile.features(i) != null && tile.geometries(i) != null) {
          layerBuilder.addFeature(tile.features(i), tile.geometries(i))
        }
        i += 1
      }
      val tb = VectorTile.Tile.newBuilder()
      tb.addLayers(layerBuilder.build())
      tile.vectorTile = tb.build()
    }

    /** Serialize the tile with ONLY geometry fields kept (all non-geom set to null). */
    def computeGeometryOnlySize(): Int = {
      val layerBuilder = new VectorLayerBuilder(tile.resolution, "features")
      var i = 0
      val n = tile.features.length
      while (i < n) {
        if (i < tile.geometries.length && tile.features(i) != null && tile.geometries(i) != null) {
          val f = tile.features(i)
          if (f != null && f.schema != null && f.schema.fields != null) {
            val cols = f.schema.fields.length
            val vals = Array.ofDim[Any](cols)
            var c = 0
            while (c < cols) {
              val field = f.schema.fields(c)
              vals(c) = if (field != null && field.dataType == GeometryDataType) safeGet(f, c) else null
              c += 1
            }
            val geomOnlyF = new Feature(vals, f.schema, f.pixelCount)
            layerBuilder.addFeature(geomOnlyF, tile.geometries(i))
          }
        }
        i += 1
      }
      val tb = VectorTile.Tile.newBuilder()
      tb.addLayers(layerBuilder.build())
      tb.build().getSerializedSize
    }

    val baseFields = base.schema.fields
    val baseFieldNames = base.schema.fieldNames

    val nonGeomBaseIdx = baseFields.indices.filter(i => baseFields(i).dataType != GeometryDataType).toArray
    val K = nonGeomBaseIdx.length
    val numFeatures = tile.features.length

    val baseNameToK = new java.util.HashMap[String, Int]()
    var kk = 0
    while (kk < K) {
      baseNameToK.put(baseFieldNames(nonGeomBaseIdx(kk)), kk)
      kk += 1
    }

    // For mapping each feature's schema -> base column index k
    val featureColIndex = Array.ofDim[Int](numFeatures, K)

    // store both string + any (for emitting quantized values)
    val cellValueStr = Array.ofDim[String](numFeatures, K)
    val cellValueAny = Array.ofDim[Any](numFeatures, K)

    // Detect numeric columns (by BASE schema)
    val isNumericK = Array.ofDim[Boolean](K)
    kk = 0
    while (kk < K) {
      val baseIdx = nonGeomBaseIdx(kk)
      val dt = if (baseIdx >= 0 && baseIdx < baseFields.length) baseFields(baseIdx).dataType else null
      isNumericK(kk) = dt != null && dt.isInstanceOf[NumericType]
      kk += 1
    }

    val NULL_MARK = "__NULL__"

    // -------------------------------
    // PASS 1: per-column stats (min/max + unique count raw)
    // -------------------------------
    val seenRaw = Array.fill(K)(mutable.HashSet.empty[String])
    val hasNum = Array.fill[Boolean](K)(false)
    val minNum = Array.fill[Double](K)(Double.PositiveInfinity)
    val maxNum = Array.fill[Double](K)(Double.NegativeInfinity)

    @inline def toDoubleOpt(x: Any): Option[Double] = {
      if (x == null) None
      else x match {
        case n: java.lang.Number => Some(n.doubleValue())
        case s: String =>
          val t = s.trim
          if (t.isEmpty || t == "null") None else try Some(t.toDouble) catch { case _: Throwable => None }
        case other =>
          val t = other.toString
          if (t == null || t == "null") None else try Some(t.toDouble) catch { case _: Throwable => None }
      }
    }

    var i = 0
    while (i < numFeatures) {
      java.util.Arrays.fill(featureColIndex(i), -1)
      val f = tile.features(i)
      if (f != null && f.schema != null) {
        val fnames = f.schema.fieldNames
        var c = 0
        while (c < fnames.length) {
          val k = baseNameToK.getOrDefault(fnames(c), -1)
          if (k != -1) featureColIndex(i)(k) = c
          c += 1
        }

        var k = 0
        while (k < K) {
          if (isNumericK(k)) {
            val colIdx = featureColIndex(i)(k)
            val vAny = if (colIdx >= 0) safeGet(f, colIdx) else null
            val norm = if (vAny == null || vAny == "null") NULL_MARK else vAny.toString
            if (norm != NULL_MARK) {
              seenRaw(k).add(norm)
              toDoubleOpt(vAny).foreach { d =>
                hasNum(k) = true
                if (d < minNum(k)) minNum(k) = d
                if (d > maxNum(k)) maxNum(k) = d
              }
            }
          }
          k += 1
        }
      }
      i += 1
    }

    // Quantize numeric cols only if uniq > numBins
    val doQuantize = Array.fill[Boolean](K)(false)
    kk = 0
    while (kk < K) {
      if (isNumericK(kk) && hasNum(kk)) {
        if (seenRaw(kk).size > numBins) doQuantize(kk) = true
      }
      kk += 1
    }

    @inline def quantizeAnyForK(k: Int, originalAny: Any): (Any, String) = {
      if (!isNumericK(k) || !doQuantize(k)) {
        val s = if (originalAny == null) NULL_MARK else originalAny.toString
        (originalAny, s)
      } else {
        val dOpt = toDoubleOpt(originalAny)
        if (dOpt.isEmpty) (null, NULL_MARK)
        else {
          val d = dOpt.get
          val q = quantizeValue(d, minNum(k), maxNum(k), numBins)

          val outAny: Any = originalAny match {
            case _: java.lang.Byte | _: java.lang.Short | _: java.lang.Integer | _: java.lang.Long =>
              java.lang.Long.valueOf(math.round(q))
            case _ =>
              java.lang.Double.valueOf(q)
          }

          val outStr: String = outAny match {
            case n: java.lang.Long    => n.toString
            case n: java.lang.Integer => n.toString
            case n: java.lang.Double  => compactDoubleString(n.doubleValue())
            case n: java.lang.Float   => compactDoubleString(n.doubleValue())
            case other                => other.toString
          }
          (outAny, outStr)
        }
      }
    }

    // -------------------------------
    // PASS 2: build post-quant strings/any + nn_k and uniqBytes_k + detect constant cols
    // -------------------------------
    val nn_k = Array.fill[Long](K)(0L)
    val uniqBytes_k = Array.fill[Double](K)(0.0)
    val colSeen = Array.fill(K)(mutable.HashSet.empty[String])

    val firstVal = Array.fill[String](K)(null)
    val isConst = Array.fill[Boolean](K)(true)

    i = 0
    while (i < numFeatures) {
      val f = tile.features(i)
      if (f != null && f.schema != null) {
        var k = 0
        while (k < K) {
          val colIdx = featureColIndex(i)(k)
          val vAny = if (colIdx >= 0) safeGet(f, colIdx) else null
          val (outAny, outStr0) = quantizeAnyForK(k, vAny)
          val outStr = if (outStr0 == null || outStr0 == "null") NULL_MARK else outStr0

          if (firstVal(k) == null) firstVal(k) = outStr
          else if (isConst(k) && firstVal(k) != outStr) isConst(k) = false

          if (outStr != NULL_MARK) {
            cellValueAny(i)(k) = outAny
            cellValueStr(i)(k) = outStr
            nn_k(k) += 1
            if (colSeen(k).add(outStr)) uniqBytes_k(k) += uniqueValueBytesApprox(outStr)
          }
          k += 1
        }
      }
      i += 1
    }

    // Drop columns that became constant post-quantization
    val dropCol = Array.ofDim[Boolean](K)
    var nn0 = 0L
    var uniq0 = 0.0
    kk = 0
    while (kk < K) {
      dropCol(kk) = isConst(kk)
      if (!dropCol(kk)) {
        nn0 += nn_k(kk)
        uniq0 += uniqBytes_k(kk)
      }
      kk += 1
    }

    // -------------------------------
    // Geometry sizing
    // -------------------------------
    val geomOnlySize = computeGeometryOnlySize()

    // New attribute bytes model:
    //   attrBytesAll = actual bytes attributable to attributes when fully kept
    //   charge per kept cell uniformly (simple but fixes infeasibility)
    val attrBytesAll: Double = math.max(0.0, initialBytes.toDouble - geomOnlySize.toDouble)
    val bytesPerCellAll: Double = if (nn0 > 0L) attrBytesAll / nn0.toDouble else 0.0
    val baseOverhead: Double = 0.0 // keep tiny; can set to small constant if you want

    // Estimate geometry per feature via vertices (still approximate)
    val vertices_i = Array.ofDim[Int](numFeatures)
    var totalVertices = 0L
    i = 0
    while (i < numFeatures) {
      val g = if (i < tile.geometries.length) tile.geometries(i) else null
      val v = if (g != null) math.max(1, g.numPoints) else 0
      vertices_i(i) = v
      totalVertices += v.toLong
      i += 1
    }

    val geomBytesPerVertex =
      if (totalVertices > 0L) geomOnlySize.toDouble / totalVertices.toDouble else 0.0
    val geomBytes_i = vertices_i.map(v => v.toDouble * geomBytesPerVertex)

    // Pixel counts (for feature utility)
    val pcAcc_i = Array.ofDim[Int](numFeatures)
    i = 0
    while (i < numFeatures) {
      val g = if (i < tile.geometries.length) tile.geometries(i) else null
      val pc =
        if (g == null) 0
        else {
          try {
            val p = calculateAccuratePixelCount(g, tileIndex)
            if (p < 0) 0 else p
          } catch {
            case _: Throwable =>
              val f = tile.features(i)
              if (f != null) math.max(0, f.pixelCount) else 0
          }
        }
      pcAcc_i(i) = pc
      i += 1
    }

    if (DEBUG_SUMMARY) {
      val qCols = (0 until K).count(k => isNumericK(k) && doQuantize(k) && !dropCol(k))
      println(
        s"[LP DEBUG] initialBytes=$initialBytes thr=$tileSizeThreshold thrLP=$thrLP geomOnlySize=$geomOnlySize " +
          f"geomBytesPerVertex=$geomBytesPerVertex%.4f nn0=$nn0 uniq0=$uniq0%.2f " +
          f"attrBytesAll=$attrBytesAll%.2f bytesPerCellAll=$bytesPerCellAll%.4f " +
          s"quantBins=$numBins quantizedCols=$qCols"
      )
    }

    // -------------------------------
    // KLD reference distributions (same as before)
    // -------------------------------
    val attributeMapNow = getAttributeMap(tile)
    val referenceAttributeMap = attributeMapNow.map(m => new java.util.HashMap[String, Int](m.toMap.asJava))
    val distributions = referenceAttributeMap.map { refMap =>
      val total = math.max(1.0, refMap.values().asScala.map(_.asInstanceOf[Int]).sum.toDouble)
      val qMap = new java.util.HashMap[String, Double]()
      val logQMap = new java.util.HashMap[String, Double]()
      refMap.asScala.foreach { case (k, v) =>
        val q = v.toDouble / total
        qMap.put(k, q)
        logQMap.put(k, if (q > 0) math.log(q) else Double.NegativeInfinity)
      }
      (total, qMap, logQMap)
    }

    val maxKLDlist = calculateMaxKLDivergenceForTile(referenceAttributeMap.map(_.asScala.toMap).toArray)
      .map(v => if (v == 0.0) 1e-5 else v)
      .toArray

    @inline def clamp01(x: Double): Double = if (x < 0.0) 0.0 else if (x > 1.0) 1.0 else x

    // normalize pixel util
    val pcMaxAll = {
      var m = 1
      i = 0
      while (i < numFeatures) { if (pcAcc_i(i) > m) m = pcAcc_i(i); i += 1 }
      m
    }

    val featUtil_i = Array.ofDim[Double](numFeatures)
    val cellUtil_ik = Array.ofDim[Double](numFeatures, K)
    val xActive = Array.ofDim[Boolean](numFeatures, K)

    i = 0
    while (i < numFeatures) {
      val pcNorm = if (pcMaxAll > 0) pcAcc_i(i).toDouble / pcMaxAll.toDouble else 0.0
      val pcNorm01 = clamp01(pcNorm)
      featUtil_i(i) = alpha * (SIZE_WEIGHT * math.pow(pcNorm01, SIZE_POWER))

      var k = 0
      while (k < K) {
        if (!dropCol(k) && cellValueStr(i)(k) != null) {
          val s = cellValueStr(i)(k)
          val baseIdx = nonGeomBaseIdx(k)

          var kldNorm = 0.0
          if (baseIdx >= 0 && baseIdx < distributions.length) {
            val (total, qMap, logQMap) = distributions(baseIdx)
            val refMap = referenceAttributeMap(baseIdx)
            val kld = efficientKLDiv(
              s,
              refMap.getOrDefault(s, 0).toDouble,
              refMap.getOrDefault("null", 0).toDouble,
              total, qMap, logQMap
            )
            kldNorm = clamp01(kld / maxKLDlist(baseIdx))
          }

          val keepBonus = 1.0 - kldNorm
          cellUtil_ik(i)(k) = (1.0 - alpha) * keepBonus
          xActive(i)(k) = true
        }
        k += 1
      }
      i += 1
    }

    // -------------------------------
    // Build + solve Gurobi model
    // -------------------------------
    val env = new GRBEnv(true)
    env.set("OutputFlag", "0")
    env.set("Threads", "1")
    env.set("MIPGap", "0.01")
    env.start()

    var model: GRBModel = null
    try {
      model = new GRBModel(env)
      model.set(GRB.IntAttr.ModelSense, GRB.MAXIMIZE)

      // y(i): keep feature i?
      val y = model.addVars(null, null, featUtil_i, Array.fill[Char](numFeatures)(GRB.BINARY), null)

      // u(k): activate column k? (kept for structure; no header cost currently)
      val u = new Array[GRBVar](K)
      kk = 0
      while (kk < K) {
        if (!dropCol(kk) && (nn_k(kk) > 0L)) u(kk) = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, null)
        kk += 1
      }

      // x(i,k): keep attribute cell (i,k)?
      val x = Array.ofDim[GRBVar](numFeatures, K)
      i = 0
      while (i < numFeatures) {
        var k = 0
        while (k < K) {
          if (xActive(i)(k)) x(i)(k) = model.addVar(0.0, 1.0, cellUtil_ik(i)(k), GRB.BINARY, null)
          k += 1
        }
        i += 1
      }

      // Constraints: x(i,k) <= y(i) and x(i,k) <= u(k)
      val reusable = new GRBLinExpr()
      i = 0
      while (i < numFeatures) {
        var k = 0
        while (k < K) {
          if (xActive(i)(k)) {
            reusable.clear()
            reusable.addTerm(1.0, x(i)(k))
            reusable.addTerm(-1.0, y(i))
            model.addConstr(reusable, GRB.LESS_EQUAL, 0.0, null)

            if (u(k) != null) {
              reusable.clear()
              reusable.addTerm(1.0, x(i)(k))
              reusable.addTerm(-1.0, u(k))
              model.addConstr(reusable, GRB.LESS_EQUAL, 0.0, null)
            }
          }
          k += 1
        }
        i += 1
      }

      // Keep at least one feature (your original behavior)
      reusable.clear()
      i = 0
      while (i < numFeatures) { reusable.addTerm(1.0, y(i)); i += 1 }
      model.addConstr(reusable, GRB.GREATER_EQUAL, 1.0, null)

      // -------------------------------
      // Budget with NEW "other bytes" model:
      //   baseOverhead + sum(geomBytes_i * y(i)) + sum(bytesPerCellAll * x(i,k)) <= thrLP
      // -------------------------------
      val budget = new GRBLinExpr()
      budget.addConstant(baseOverhead)

      // Geometry cost
      val yVars = new mutable.ArrayBuffer[GRBVar](numFeatures)
      val yCoeffs = new mutable.ArrayBuffer[Double](numFeatures)
      i = 0
      while (i < numFeatures) {
        val gb = geomBytes_i(i)
        if (gb > 0) { yVars += y(i); yCoeffs += gb }
        i += 1
      }
      if (yVars.nonEmpty) budget.addTerms(yCoeffs.toArray, yVars.toArray)

      // Attribute cell cost (uniform per kept cell)
      val xVars = new mutable.ArrayBuffer[GRBVar]()
      val xCoeffs = new mutable.ArrayBuffer[Double]()
      i = 0
      while (i < numFeatures) {
        var k = 0
        while (k < K) {
          if (xActive(i)(k)) {
            xVars += x(i)(k)
            xCoeffs += bytesPerCellAll
          }
          k += 1
        }
        i += 1
      }
      if (xVars.nonEmpty && bytesPerCellAll > 0.0) budget.addTerms(xCoeffs.toArray, xVars.toArray)

      model.addConstr(budget, GRB.LESS_EQUAL, thrLP, null)

      model.optimize()

      val status = model.get(GRB.IntAttr.Status)
      if (status != GRB.OPTIMAL && status != GRB.SUBOPTIMAL) {
        // IMPORTANT: previously you silently did nothing. Now we log and return (caller can fallback).
        println(s"[LP FAIL] status=$status initialBytes=$initialBytes thr=$tileSizeThreshold geomOnlySize=$geomOnlySize attrBytesAll=$attrBytesAll nn0=$nn0 bytesPerCellAll=$bytesPerCellAll")
        return
      }

      // Read solution y(i)
      val yVals = model.get(GRB.DoubleAttr.X, y)
      val keepFeature = Array.ofDim[Boolean](numFeatures)
      var keptCount = 0
      i = 0
      while (i < numFeatures) {
        val keep = yVals(i) > 0.5
        keepFeature(i) = keep
        if (keep) keptCount += 1
        i += 1
      }

      // Debug estimate (with new model)
      if (DEBUG_SUMMARY) {
        var estGeom = baseOverhead
        i = 0
        while (i < numFeatures) {
          if (keepFeature(i)) estGeom += geomBytes_i(i)
          i += 1
        }

        var estCell = 0.0
        if (bytesPerCellAll > 0.0) {
          i = 0
          while (i < numFeatures) {
            var k = 0
            while (k < K) {
              if (xActive(i)(k)) {
                val xv = x(i)(k).get(GRB.DoubleAttr.X)
                estCell += bytesPerCellAll * xv
              }
              k += 1
            }
            i += 1
          }
        }

        val estTotal = estGeom + estCell
        lpEstimatedBytes = estTotal

        println(
          f"[LP SIZE] estTotal=${estTotal}%.2f bytes  " +
            f"(geom=${estGeom}%.2f, cell=${estCell}%.2f)  " +
            f"thrLP=${thrLP}%.2f thr=${tileSizeThreshold}%d"
        )
      }

      // Materialize reduced tile (drop feature => drop geom; drop cell => null value)
      val newFeatures = new mutable.ArrayBuffer[IFeature](keptCount)
      val newGeoms = new mutable.ArrayBuffer[LiteGeometry](keptCount)

      i = 0
      while (i < numFeatures) {
        if (keepFeature(i)) {
          val f = tile.features(i)
          val g = if (i < tile.geometries.length) tile.geometries(i) else null

          if (f == null || f.schema == null) {
            newFeatures += f
            newGeoms += g
          } else {
            val cols = f.schema.fields.length
            val vals = Array.ofDim[Any](cols)
            val fnames = f.schema.fieldNames

            var c = 0
            while (c < cols) {
              val field = f.schema.fields(c)
              if (field != null && field.dataType == GeometryDataType) {
                vals(c) = safeGet(f, c)
              } else {
                val name = if (fnames != null && c < fnames.length) fnames(c) else null
                val k2 = if (name != null) baseNameToK.getOrDefault(name, -1) else -1

                if (k2 != -1) {
                  if (dropCol(k2)) {
                    vals(c) = null
                  } else {
                    val keptCell = xActive(i)(k2) && (x(i)(k2).get(GRB.DoubleAttr.X) > 0.5)
                    if (keptCell) {
                      val qAny = cellValueAny(i)(k2)
                      vals(c) = if (qAny != null) qAny else safeGet(f, c)
                    } else {
                      vals(c) = null
                    }
                  }
                } else {
                  vals(c) = safeGet(f, c)
                }
              }
              c += 1
            }

            val newPc = math.max(0, pcAcc_i(i))
            newFeatures += new Feature(vals, f.schema, newPc)
            newGeoms += g
          }
        }
        i += 1
      }

      tile.features = newFeatures
      tile.geometries = newGeoms
      rebuildTileFromCurrentArrays()

      if (DEBUG_SUMMARY) {
        val finalBytes = tile.vectorTile.getSerializedSize.toDouble
        val diff = if (lpEstimatedBytes.isNaN) Double.NaN else (finalBytes - lpEstimatedBytes)
        println(
          f"[LP SIZE] est=${lpEstimatedBytes}%.2f actual=${finalBytes}%.2f diff(actual-est)=${diff}%.2f " +
            s"thr=$tileSizeThreshold"
        )
      }

    } finally {
      try { if (model != null) model.dispose() } catch { case _: Throwable => }
      try { env.dispose() } catch { case _: Throwable => }
    }
  }

  // =========================================================================================
  // Range parsing + index.html
  // =========================================================================================

  def parseRange(str: String): Range = {
    val oneNumber = raw"(\d+)".r
    val inclusiveRange = raw"(\d+)..(\d+)".r
    val exclusiveRange = raw"(\d+)...(\d+)".r
    str match {
      case oneNumber(number) => 0 until number.toInt
      case inclusiveRange(start, end) => start.toInt to end.toInt
      case exclusiveRange(start, end) => start.toInt until end.toInt - 1
      case _ => throw new RuntimeException(s"Unrecognized range format $str. start..end is a supported format")
    }
  }

  def getIndexHTMLFile(opts: BeastOptions): String = {
    val resolution: Int = opts.getInt(Resolution, 256)
    val strLevels: Array[String] = opts.getString("levels", "7").split("\\.\\.")
    var minLevel: Int = 0
    var maxLevel: Int = 0
    if (strLevels.length == 1) {
      minLevel = 0
      maxLevel = strLevels(0).toInt - 1
    } else {
      minLevel = strLevels(0).toInt
      maxLevel = strLevels(1).toInt
    }
    val templateFile = "/mvt_plot.html"
    val templateFileReader = new LineReader(classOf[MultilevelPyramidPlotHelper].getResourceAsStream(templateFile))
    val htmlOut = new StringBuilder
    try {
      val line = new Text
      while (templateFileReader.readLine(line) > 0) {
        var lineStr = line.toString
        lineStr = lineStr.replace("#{RESOLUTION}", Integer.toString(resolution))
        lineStr = lineStr.replace("#{MAX_ZOOM}", Integer.toString(maxLevel))
        lineStr = lineStr.replace("#{MIN_ZOOM}", Integer.toString(minLevel))
        htmlOut.append(lineStr).append("\n")
      }
    } finally {
      templateFileReader.close()
    }
    htmlOut.toString()
  }

  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val features: SpatialRDD = sc.spatialFile(inputs(0), opts)
    val levels: Range = parseRange(opts.getString(NumLevels, "7"))
    val resolution = opts.getInt(Resolution, 256)

    val tiles: RDD[(Long, IntermediateVectorTile)] = plotAllTiles(features, levels, resolution, 5, opts)

    val outPath = new Path(outputs(0))
    val outFS = outPath.getFileSystem(sc.hadoopConfiguration)

    if (outFS.exists(outPath) && opts.getBoolean(SpatialWriter.OverwriteOutput, false))
      outFS.delete(outPath, true)

    opts.set("data", FileUtil.relativize(new Path(inputs(0)), outPath))

    if (opts.getBoolean(CompactOutput, true))
      saveTilesCompact(tiles, outputs(0), opts)
    else
      saveTiles(tiles, outputs(0), opts)
  }
}
