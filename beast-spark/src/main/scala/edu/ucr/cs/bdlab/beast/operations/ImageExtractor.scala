package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast.cg.{Reprojector, SpatialPartitioner}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, BlockCartesianRDD, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, Feature, IFeature, ITile}
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper.NumPartitions
import edu.ucr.cs.bdlab.beast.indexing.{IndexHelper, RSGrovePartitioner}
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import edu.ucr.cs.bdlab.beast.{RasterRDD, _}
import edu.ucr.cs.bdlab.raptor.ZonalStatistics.RasterLayer
import edu.ucr.cs.bdlab.raptor._
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.locationtech.jts.geom.Envelope

import java.awt.Color
import java.awt.geom.Point2D
import java.awt.image.BufferedImage
import java.io.{IOException, OutputStream, PrintStream}
import java.util.zip.Deflater
import javax.imageio.ImageIO
import scala.collection.mutable.ArrayBuffer

/**
 * Joins a raster image with a set of polygons and extracts a separate image for each polygon.
 * Each image is resized to a given fixed size, e.g., 256 x 256. This makes it helpful to use the extracted
 * images in machine learning algorithms which expect input images to be of the same size.
 */
@OperationMetadata(shortName = "imagex",
  description = "Extracts images for a set of geometries from another set of geospatial images",
  inputArity = "2",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat]))
object ImageExtractor extends CLIOperation with Logging {

  @OperationParam(description = "The resolution of the output images in pixels", defaultValue = "256")
  val ImageResolution: String = "resolution"

  @OperationParam(description = "Keep the aspect ratio of the input geometry in output images", defaultValue = "true")
  val KeepAspectRatio: String = "keepratio"

  @OperationParam(
    description = "The attribute to use as file name. If not set, a unique arbitrary ID will be given to each geometry",
    required = false)
  val FileNameAttribute: String = "filenameattr"

  @OperationParam(description = "Enable this option to write images in ZIP archives to reduce number of files",
    defaultValue = "false")
  val ZipImages: String = "zipimages"

  @OperationParam(description = "The buffer size in pixels to take around non-polygonal geometries",
    defaultValue = "10")
  val BufferSize: String = "buffersize"

  @OperationParam(description = "Number of rounds to generate the images in", defaultValue = "1")
  val Rounds: String = "rounds"

  @OperationParam(description = "The grain size of the Raptor result: tile or pixel", defaultValue = "tile")
  val RaptorResult: String = "raptorresult"

  override def printUsage(out: PrintStream): Unit = {
    out.println("Given a set of images (rasters) and a set of features (vectors), this method extracts an image")
    out.println("for each feature that represents the overlapping pixels in the raster data.")
    out.println("The two inputs are raster and vector.")
    out.println("The output directory will contain one image for each feature that overlaps at least one pixel.")
  }

  /**
   * Run the main function using the given user command-line options and spark context
   *
   * @param opts    user options for configuring the operation
   * @param inputs  inputs provided by the user
   * @param outputs outputs provided by the user
   * @param sc      the Spark context used to run the operation
   * @return an optional result of this operation
   */
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val iLayer = opts.getString(RasterLayer, "0")
    opts.set(IRasterReader.RasterLayerID, iLayer)
    val rasters: RDD[ITile[Array[Int]]] = new RasterFileRDD[Array[Int]](sc, inputs(0), opts.retainIndex(0))

    val allFeatures = sc.spatialFile(inputs(1), opts.retainIndex(1))
      .filter(_.getGeometry != null)
    val featuresWithIDs: RDD[(Long, IFeature)] = allFeatures
      .zipWithUniqueId().map(x => (x._2, x._1))
    val rounds = opts.getInt(Rounds, 1)
    val partitioner: SpatialPartitioner = if (rounds > 1) {
      IndexHelper.createPartitioner(allFeatures, classOf[RSGrovePartitioner],
        NumPartitions(IndexHelper.Fixed, rounds), _ => 1, opts)
    } else {
      null
    }
    for (round <- 0 until rounds) {
      var features = if (partitioner != null)
        featuresWithIDs.filter(f => partitioner.overlapPartition(new EnvelopeNDLite().merge(f._2.getGeometry)) == round)
      else
        featuresWithIDs
      // Use the metadata of the first raster to determine the pixel size in world coordinates of the raster
      val rasterMetadata = rasters.first().rasterMetadata
      val pixelSizeInRaster: Double = rasterMetadata.getPixelScaleX
      val range = Array[Double](0, 0, pixelSizeInRaster, pixelSizeInRaster)
      val transform = Reprojector.findTransformationInfo(rasterMetadata.srid, features.first._2.getGeometry.getSRID)
      transform.mathTransform.transform(range, 0, range, 0, 2)
      val pixelSizeInVector: Double = (range(2) - range(0)).abs
      val bufferSize: Double = pixelSizeInVector * opts.getInt(BufferSize, 10)
      val fileNameAttribute: String = opts.getString(FileNameAttribute)
      val filenames: RDD[(Long, String)] = if (fileNameAttribute == null)
        null
      else
        features.map(x => (x._1, x._2.getAs(fileNameAttribute).toString))
      features = features.map(f => (f._1, f._2.getGeometry.getGeometryType match {
        case "Point" | "MultiPoint" | "LineString" | "MultiLineString" =>
          Feature.create(null, f._2.getGeometry.buffer(bufferSize))
        case _ => Feature.create(null, f._2.getGeometry)
      }))

      val numRanges: LongAccumulator = features.sparkContext.longAccumulator("num ranges")
      val numCrossTiff: LongAccumulator = features.sparkContext.longAccumulator("numCrossTiff")
      val numTooManyTiles: LongAccumulator = features.sparkContext.longAccumulator("numTooManyTiles")
      val numTooLessPixels: LongAccumulator = features.sparkContext.longAccumulator("numTooLessPixels")
      var images: RDD[(Long, Array[Byte])] = null
      val raptorResult: String = opts.getString(RaptorResult, "tile")
      if (raptorResult == "tile") {
        images = extractImagesTile(features, rasters, opts, numRanges)
      } else if (raptorResult == "pixel") {
        images = extractImages(features, rasters, opts, numRanges)
      } else if (raptorResult == "tileItr") {
        images = extractImagesTileIterate(features, rasters, opts, numRanges,
          numCrossTiff, numTooManyTiles, numTooLessPixels)
      }
      val outputDir: String = outputs(0)
      val imagesWithNames: RDD[(String, Array[Byte])] = if (filenames == null)
        images.map(x => (x._1.toString, x._2))
      else
        filenames.join(images).map(x => x._2)
      val hadoopConfCopy: BeastOptions = new BeastOptions(opts.loadIntoHadoopConf(sc.hadoopConfiguration))
      if (opts.getBoolean(ZipImages, false)) {
        // Combine images in archives to reduce number of files
        imagesWithNames.foreachPartition(images => {
          if (images.hasNext) {
            // TODO get partition number to avoid creating random file names
            val outPath = new Path(outputDir)
            val fileSystem = outPath.getFileSystem(hadoopConfCopy.loadIntoHadoopConf(null))
            var outputFile: OutputStream = null
            var attempts: Int = 0
            do {
              try {
                outputFile = fileSystem.create(new Path(outPath, (Math.random() * 1000000000).toInt + ".zip"))
              } catch {
                case _: IOException => attempts += 1
              }
            } while (outputFile == null && attempts < 50)
            if (outputFile == null)
              throw new RuntimeException(s"Failed to create an output directory after $attempts attempts")
            val zipOutput: ZipArchiveOutputStream = new ZipArchiveOutputStream(outputFile)
            // Disable compression since PNG images are already compressed
            zipOutput.setLevel(Deflater.NO_COMPRESSION)
            images.foreach(f => {
              // Write the image to the output
              import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
              val entry = new ZipArchiveEntry(f._1 + ".png")
              zipOutput.putArchiveEntry(entry)
              zipOutput.write(f._2)
              zipOutput.closeArchiveEntry()
            })
            zipOutput.close()
          }
        })
      } else {
        // Write each image as a single file
        imagesWithNames.foreach(x => {
          // Write the image to the output
          val imagePath = new Path(outputDir, x._1 + ".png")
          val filesystem = imagePath.getFileSystem(new Configuration())
          val out = filesystem.create(imagePath)
          out.write(x._2)
          out.close()
        })
      }
      logInfo(s"Processed a total of ${numRanges.value} ranges")
      logInfo(s"Processed a total of ${numCrossTiff.value} CrossTiff geometries," +
        s"${numTooManyTiles.value} TooManyTiles geometries and ${numTooLessPixels.value} TooLessPixels geometries")
    }
  }

  /**
   * Extracts images for the given set of vector and raster data.
   *
   * @param geometries a set of (id, geometry) pairs to extract images for
   * @param rasters    an RDD of raster images. It is expected that the pixel value is either three bands (RGB) or
   *                   four bands (RGBA)
   * @param opts       additional options to configure the image extraction process
   * @return an RDD of (id, image) pairs. Since [[BufferedImage]] is not serializable, the bytes represent a PNG image
   *         that is already serialized. This allows the return value to be joined back with features if needed.
   */
  def extractImages(features: RDD[(Long, IFeature)], rasters: RasterRDD[Array[Int]], opts: BeastOptions,
                    numRanges: LongAccumulator = null): RDD[(Long, Array[Byte])] = {
    // 1- Perform a raptor join between the raster and vector data
    val joinResults: RDD[RaptorJoinResult[Array[Int]]] =
      RaptorJoin.raptorJoinIDFull(rasters, features, new BeastOptions(), numRanges = numRanges)

    // 2- Get polygon minimum bounding rectangles (MBRs) in the raster coordinate reference system (CRS)
    val vectorSRID = features.first._2.getGeometry.getSRID

    val sparkConf: SparkConf = features.sparkContext.getConf
    val geomMBRs: RDD[(Long, Envelope)] = features.map(x => (x._1, x._2.getGeometry.getEnvelopeInternal))

    // 3- Map input values to pixel values in the image space
    // 3.1- Convert each pixel to a square in the world space
    val geometryPixelsInWorld: RDD[(Long, (Float, Float, Float, Float, Int))] = joinResults.map(result => {
      // For each pixel, keep its geographic location and color
      val point2D1 = new Point2D.Double()
      result.rasterMetadata.gridToModel(result.x, result.y, point2D1)
      val point2D2 = new Point2D.Double()
      result.rasterMetadata.gridToModel(result.x + 1.0, result.y + 1.0, point2D2)
      val color = new Color(result.m(0), result.m(1), result.m(2)).getRGB
      if (result.rasterMetadata.srid != vectorSRID) {
        val transformInfo = Reprojector.findTransformationInfo(result.rasterMetadata.srid, vectorSRID)
        val points: Array[Float] = Array[Float](point2D1.x.toFloat, point2D1.y.toFloat,
          point2D2.x.toFloat, point2D2.y.toFloat)
        transformInfo.mathTransform.transform(points, 0, points, 0, 2)
        (result.featureID, (points(0), points(1), points(2), points(3), color))
      } else
        (result.featureID, (point2D1.x.toFloat, point2D1.y.toFloat, point2D2.x.toFloat, point2D2.y.toFloat, color))
    })

    // 3.2- Join with geometry MBRs to convert the square into the image space in pixels
    val outputResolution: Int = opts.getInt(ImageResolution, 256)
    val keepAspectRatio: Boolean = opts.getBoolean(KeepAspectRatio, true)
    val pixelProcessor: (Iterator[(Long, Envelope)], Iterator[(Long, (Float, Float, Float, Float, Int))]) => Iterator[(Long, (Int, Int, Int, Int, Int))] = {
      case (someGeometryMBRs: Iterator[(Long, Envelope)], pixels: Iterator[(Long, (Float, Float, Float, Float, Int))]) =>
        val geometryMBRsLocal: Map[Long, Envelope] = someGeometryMBRs.toMap
        pixels.map({ case (featureID: Long, (x1: Float, y1: Float, x2: Float, y2: Float, color: Int)) =>
          val mbro = geometryMBRsLocal.get(featureID)
          if (mbro.isEmpty)
            null
          else {
            val mbr = mbro.get
            var xRatio = outputResolution / mbr.getWidth
            var yRatio = outputResolution / mbr.getHeight
            if (keepAspectRatio) {
              xRatio = xRatio min yRatio
              yRatio = xRatio
            }
            val x1P: Int = ((x1 - mbr.getMinX) * xRatio).toInt max 0
            val x2P: Int = ((x2 - mbr.getMinX) * xRatio).toInt min (outputResolution - 1)
            val y1P: Int = (outputResolution - 1 - ((y1 - mbr.getMinY) * yRatio)).toInt max 0
            val y2P: Int = (outputResolution - 1 - ((y2 - mbr.getMinY) * yRatio)).toInt min (outputResolution - 1)

            (featureID, (x1P, y1P, x2P, y2P, color))
          }
        }).filterNot(x => x == null || x._2._1 == x._2._3 || x._2._2 == x._2._4)
    }
    // Run hash join if the combined number of partitions is too large
    val hashJoin = geomMBRs.getNumPartitions * geometryPixelsInWorld.getNumPartitions > 2 * features.sparkContext.defaultParallelism
    logDebug(s"Using ${if (hashJoin) "hash join" else "BNLJ"}")
    val countryPixelsInImage: RDD[(Long, (Int, Int, Int, Int, Int))] =
      if (!hashJoin) {
        // Apply block nested loop join
        val combinedPartitions: RDD[(Iterator[(Long, Envelope)], Iterator[(Long, (Float, Float, Float, Float, Int))])] =
          new BlockCartesianRDD(features.sparkContext, geomMBRs, geometryPixelsInWorld)
        combinedPartitions.flatMap({
          case (someCountryMBRs: Iterator[(Long, Envelope)], pixels: Iterator[(Long, (Float, Float, Float, Float, Int))]) =>
            pixelProcessor(someCountryMBRs, pixels)
        })
      } else {
        // Apply hash join
        val hashPartitioner = new HashPartitioner(geomMBRs.getNumPartitions max geometryPixelsInWorld.getNumPartitions max geomMBRs.sparkContext.defaultParallelism)
        val partitionedGeomMBRs = geomMBRs.partitionBy(hashPartitioner)
        val partitionedPixels = geometryPixelsInWorld.partitionBy(hashPartitioner)

        partitionedGeomMBRs.zipPartitions(partitionedPixels, true)(pixelProcessor)
      }

    // 5- Arrange pixels into an array that resembles the output image and create the image data
    val emptyPixels: Array[Int] = new Array[Int](outputResolution * outputResolution)

    val countryImages: RDD[(Long, Array[Byte])] = countryPixelsInImage.aggregateByKey(emptyPixels,
      new HashPartitioner(countryPixelsInImage.sparkContext.defaultParallelism))((pixels, pixel) => {
      for (x <- pixel._1 to pixel._3; y <- pixel._2 to pixel._4) {
        val offset = y * outputResolution + x
        pixels(offset) = pixel._5
      }
      pixels
    }, (pixels1, pixels2) => {
      for (i <- pixels1.indices; if pixels1(i) == 0)
        pixels1(i) = pixels2(i)
      pixels1
    }).mapPartitions(_.map({ case (id: Long, pixels: Array[Int]) =>
      // Combine pixels into an image using Java imaging API
      val image = new BufferedImage(outputResolution, outputResolution, BufferedImage.TYPE_INT_ARGB)
      for (x <- 0 until outputResolution; y <- 0 until outputResolution) {
        val offset = y * outputResolution + x
        image.setRGB(x, y, pixels(offset))
      }
      // Convert it to PNG format
      val baos = new ByteArrayOutputStream()
      ImageIO.write(image, "png", baos)
      baos.close()
      (id, baos.toByteArray)
    }), preservesPartitioning = true)

    countryImages
  }

  /**
   * Extracts images for the given set of vector and raster data.
   *
   * @param geometries a set of (id, geometry) pairs to extract images for
   * @param rasters    an RDD of raster images. It is expected that the pixel value is either three bands (RGB) or
   *                   four bands (RGBA)
   * @param opts       additional options to configure the image extraction process
   * @return an RDD of (id, image) pairs. Since [[BufferedImage]] is not serializable, the bytes represent a PNG image
   *         that is already serialized. This allows the return value to be joined back with features if needed.
   */
  def extractImagesTile(features: RDD[(Long, IFeature)], rasters: RasterRDD[Array[Int]], opts: BeastOptions,
                        numRanges: LongAccumulator = null): RDD[(Long, Array[Byte])] = {
    // 1- Perform a raptor join between the raster and vector data
    val joinResults: RDD[RaptorJoinResultTile[Array[Int]]] =
      RaptorJoin.raptorJoinIDFullTile(rasters, features, new BeastOptions(), numRanges = numRanges)

    extractImagesWJoinResults(joinResults, features, opts)
  }

  def extractImagesWJoinResults(joinResults: RDD[RaptorJoinResultTile[Array[Int]]], features: RDD[(Long, IFeature)],
                                opts: BeastOptions): RDD[(Long, Array[Byte])] = {
    if (features.isEmpty()) return features.sparkContext.emptyRDD
    // 2- Get polygon minimum bounding rectangles (MBRs) in the raster coordinate reference system (CRS)
    val vectorSRID = features.first._2.getGeometry.getSRID

    val sparkConf: SparkConf = features.sparkContext.getConf
    val geomMBRs: RDD[(Long, Envelope)] = features.map(x => (x._1, x._2.getGeometry.getEnvelopeInternal))

    val geometryTile: RDD[(Long, ITile[Array[Int]])] = joinResults.map(result => (result.featureID, result.tile))

    // 3- Map input values to pixel values in the image space
    // 3.1- Convert each pixel to a square in the world space
    // 3.2- Join with geometry MBRs to convert the square into the image space in pixels
    val outputResolution: Int = opts.getInt(ImageResolution, 256)
    val keepAspectRatio: Boolean = opts.getBoolean(KeepAspectRatio, true)
    val tileProcessor: (Iterator[(Long, Envelope)], Iterator[(Long, ITile[Array[Int]])]) =>
      Iterator[(Long, Array[(Int, Int, Int, Int, Int)])] = {
      case (someGeometryMBRs: Iterator[(Long, Envelope)], tiles: Iterator[(Long, ITile[Array[Int]])]) =>
        val geometryMBRsLocal: Map[Long, Envelope] = someGeometryMBRs.toMap
        tiles.map({ case (featureID: Long, tile: ITile[Array[Int]]) =>
          val mbro = geometryMBRsLocal.get(featureID)
          if (mbro == null || mbro.isEmpty)
            null
          else {
            val mbr = mbro.get
            var xRatio = outputResolution / mbr.getWidth
            var yRatio = outputResolution / mbr.getHeight
            if (keepAspectRatio) {
              xRatio = xRatio min yRatio
              yRatio = xRatio
            }
            val res: ArrayBuffer[(Int, Int, Int, Int, Int)] = new ArrayBuffer[(Int, Int, Int, Int, Int)]()

            for (x <- tile.x1 to tile.x2; y <- tile.y1 to tile.y2) {
              // For each pixel, keep its geographic location and color
              if (!tile.isEmpty(x, y)) {
                val point2D1 = new Point2D.Double()
                tile.rasterMetadata.gridToModel(x, y, point2D1)
                val point2D2 = new Point2D.Double()
                tile.rasterMetadata.gridToModel(x + 1.0, y + 1.0, point2D2)
                val pixelValue = tile.getPixelValue(x, y)
                val color = new Color(pixelValue(0), pixelValue(1), pixelValue(2)).getRGB
                val points: Array[Float] = Array[Float](point2D1.x.toFloat, point2D1.y.toFloat,
                  point2D2.x.toFloat, point2D2.y.toFloat)
                if (tile.rasterMetadata.srid != vectorSRID) {
                  val transformInfo = Reprojector.findTransformationInfo(tile.rasterMetadata.srid, vectorSRID)
                  transformInfo.mathTransform.transform(points, 0, points, 0, 2)
                }

                val x1P: Int = ((points(0) - mbr.getMinX) * xRatio).toInt max 0
                val x2P: Int = ((points(2) - mbr.getMinX) * xRatio).toInt min (outputResolution - 1)
                val y1P: Int = (outputResolution - 1 - ((points(1) - mbr.getMinY) * yRatio)).toInt max 0
                val y2P: Int = (outputResolution - 1 - ((points(3) - mbr.getMinY) * yRatio)).toInt min (outputResolution - 1)
                if (x1P != x2P && y1P != y2P) {
                  res.+=((x1P, y1P, x2P, y2P, color))
                }
              }
            }

            (featureID, res.toArray)
          }
        }).filterNot(x => x == null)
    }
    // Run hash join if the combined number of partitions is too large
    val hashJoin = geomMBRs.getNumPartitions * geometryTile.getNumPartitions > 2 * features.sparkContext.defaultParallelism
    logDebug(s"Using ${if (hashJoin) "hash join" else "BNLJ"}")
    val countryPixelsInImage: RDD[(Long, Array[(Int, Int, Int, Int, Int)])] =
      if (!hashJoin) {
        // Apply block nested loop join
        val combinedPartitions: RDD[(Iterator[(Long, Envelope)], Iterator[(Long, ITile[Array[Int]])])] =
          new BlockCartesianRDD(features.sparkContext, geomMBRs, geometryTile)
        combinedPartitions.flatMap({
          case (someCountryMBRs: Iterator[(Long, Envelope)], pixels: Iterator[(Long, ITile[Array[Int]])]) =>
            tileProcessor(someCountryMBRs, pixels)
        })
      } else {
        // Apply hash join
        val hashPartitioner = new HashPartitioner(geomMBRs.getNumPartitions max geometryTile.getNumPartitions max geomMBRs.sparkContext.defaultParallelism)
        val partitionedGeomMBRs = geomMBRs.partitionBy(hashPartitioner)
        val partitionedPixels = geometryTile.partitionBy(hashPartitioner)

        partitionedGeomMBRs.zipPartitions(partitionedPixels, true)(tileProcessor)
      }

    // 5- Arrange pixels into an array that resembles the output image and create the image data
    val emptyPixels: Array[Int] = new Array[Int](outputResolution * outputResolution)

    val countryImages: RDD[(Long, Array[Byte])] = countryPixelsInImage.aggregateByKey(emptyPixels,
      new HashPartitioner(countryPixelsInImage.sparkContext.defaultParallelism))((pixels, pixelArray) => {
      for (i <- pixelArray) {
        for (x <- i._1 to i._3; y <- i._2 to i._4) {
          val offset = y * outputResolution + x
          pixels(offset) = i._5
        }
      }
      pixels
    }, (pixels1, pixels2) => {
      for (i <- pixels1.indices; if pixels1(i) == 0)
        pixels1(i) = pixels2(i)
      pixels1
    }).mapPartitions(_.map({ case (id: Long, pixels: Array[Int]) =>
      // Combine pixels into an image using Java imaging API
      val image = new BufferedImage(outputResolution, outputResolution, BufferedImage.TYPE_INT_ARGB)
      for (x <- 0 until outputResolution; y <- 0 until outputResolution) {
        val offset = y * outputResolution + x
        image.setRGB(x, y, pixels(offset))
      }
      // Convert it to PNG format
      val baos = new ByteArrayOutputStream()
      ImageIO.write(image, "png", baos)
      baos.close()
      (id, baos.toByteArray)
    }), preservesPartitioning = true)

    countryImages
  }

  /**
   * Extracts images for the given set of vector and raster data using repartitionAndSort and ImageIterator.
   *
   * @param geometries a set of (id, geometry) pairs to extract images for
   * @param rasters    an RDD of raster images. It is expected that the pixel value is either three bands (RGB) or
   *                   four bands (RGBA)
   * @param opts       additional options to configure the image extraction process
   * @return an RDD of (id, image) pairs. Since [[BufferedImage]] is not serializable, the bytes represent a PNG image
   *         that is already serialized. This allows the return value to be joined back with features if needed.
   */
  def extractImagesTileIterate(features: RDD[(Long, IFeature)], rasters: RasterRDD[Array[Int]], opts: BeastOptions,
                               numRanges: LongAccumulator = null,
                               numCrossTiff: LongAccumulator,
                               numTooManyTiles: LongAccumulator,
                               numTooLessPixels: LongAccumulator): RDD[(Long, Array[Byte])] = {
    val joinResults: RDD[RaptorJoinResultTile[Array[Int]]] =
      RaptorJoin.raptorJoinIDFullTile(rasters, features, new BeastOptions(), numRanges = numRanges)

    val images = joinResults.map(x => (x.featureID, x.tile))
      .repartitionAndSortWithinPartitions(new HashPartitioner(joinResults.getNumPartitions))
      .mapPartitions(iter => new ImageIterator(iter, numCrossTiff, numTooManyTiles, numTooLessPixels))
      .persist()

    extractImagesWJoinResults(joinResults, features.join(images.filter(x => x._2 == null)).map(x => (x._1, x._2._1)), opts)
      .union(images.filter(x => x._2 != null))
  }
}
