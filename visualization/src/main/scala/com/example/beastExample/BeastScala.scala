package com.example.beastExample

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin.ReadWriteMixinFunctions
import edu.ucr.cs.bdlab.davinci.{MVTDataVisualizer, VectorTile}
import org.apache.spark.SparkConf
import org.apache.spark.beast.{CRSServer, SparkSQLRegistration}
import org.apache.spark.sql.SparkSession

import java.io.{BufferedOutputStream, ByteArrayInputStream, FileOutputStream, PrintStream}
import java.nio.file.{Files, Paths}
import java.util.zip.GZIPInputStream
/**
 * Scala examples for Beast
 */
object BeastScala {

  def redirectAllOutputToFile[R](fileName: String)(block: => R): R = {
    val originalOut = System.out
    val originalErr = System.err
    val fileOut = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName, false))) // Open in append mode with auto-flush
    try {
      System.setOut(fileOut)
      System.setErr(fileOut)
      val result = block
      fileOut.flush() // Ensure all output is written to the file
      result
    } finally {
      fileOut.close()
      System.setOut(originalOut)
      System.setErr(originalErr)
    }
  }

  def main(args: Array[String]): Unit = {
    //redirectAllOutputToFile("application_log.txt") {
      // All output in this block will be redirected to "application_log.txt"
     // println("This is a test message.")
    // Initialize Spark context
    val startTime = System.nanoTime()
    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    //val sparkConf = new SparkConf()

    // Added these two lines to the template to remove some error:
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[edu.ucr.cs.bdlab.beast.util.BitArray]))



    // Start the CRSServer and store the information in SparkConf
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    CRSServer.startServer(sparkContext)
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    try {
      // Import Beast features
      // TODO Insert your code here
      val opts: BeastOptions = "threshold" -> 100000 //materialization threshold

      // --- ADD THIS TO RUN THE BENCHMARK ---
      //println("--- Starting Standalone Heap Benchmark ---")
      //val geojsonPath = "/Users/tarlan/Downloads/Quebec_small_10k.geojson" // adjust path
      //val geojsonPath = "/Users/tarlan/Downloads/TIGER2018_ADDRFEAT.geojson"
      //edu.ucr.cs.bdlab.davinci.HeapPerfBenchNoSpark.benchHeapOnGeoJSON(
        //geojsonPath = geojsonPath,
        //samplerSpec = "mode=sort priority=vertices:largest capacity=cells:1000000",
        //progressEvery = 100000 // adjust based on file size
      //)
      //println("--- End Standalone Heap Benchmark ---")
      //val features = sparkContext.spatialFile("checkins.txt") // points dataset
      //val features = sparkContext.geojsonFile("/Users/tarlan/Downloads/IdMyPhBuildings.geojson")
      //val features = sparkContext.shapefile("roads.zip")
      //val features = sparkContext.shapefile("minerals.zip")
      //val features = sparkContext.geojsonFile("Crimes_-_2022.csv")
      //val features = sparkContext.readCSVPoint(filename ="/Users/tarlan/Downloads/cleaned_ebird1M.csv" , xColumn = 0, yColumn =1 )
      //val features = sparkContext.shapefile("/Users/tarlan/Downloads/Chicago_Crimes/chicago0.zip")
      //val features = sparkContext.shapefile("checkins.zip")
      //val features = sparkContext.shapefile("provinces.zip")
      //val features = sparkContext.shapefile("/Users/tarlan/Downloads/TIGER2018_COUNTY")
      //val features = sparkContext.geojsonFile("/Users/tarlan/Downloads/Quebec_small_10k.geojson")
      //val features = sparkContext.shapefile("/Users/tarlan/Downloads/archive-2/ne_COUNTRIES.zip")
      //val features = sparkContext.shapefile("/Users/tarlan/Downloads/NE_populated_places/Archive.zip")
      //val features = sparkContext.shapefile("/Users/tarlan/Downloads/NE_roads/Archive.zip")
      //val features = sparkContext.geojsonFile("/Users/tarlan/Downloads/subset_0.1.geojson") //roads
      //val features = sparkContext.geojsonFile("/Users/tarlan/Downloads/TIGER2018_COUNTY.geojson")
      val features = sparkContext.geojsonFile("/Users/tarlan/Downloads/NE_roads_NA.geojson")
      //val features = sparkContext.geojsonFile("/Users/tarlan/Downloads/ebird_subset0001.geojson")
      //val features = sparkContext.shapefile("/Users/tarlan/Downloads/Ahmadi17_AMS_road/ams_road_vertices.zip")
      val tiles = MVTDataVisualizer.plotAllTiles(features,  levels=0 to 12, resolution=256, buffer=5, opts)
      //MVTDataVisualizer.saveTiles(tiles, "provinces_NOV29__sort_vertices-largest_kb1024_thr1024kb_redthr128kb_k5000", opts)
      //MVTDataVisualizer.saveTiles(tiles, "provinces_Hi5_zoom8", opts)

      //MVTDataVisualizer.saveTiles(tiles, "ebird_subset0001_redthr_4096", opts)
      MVTDataVisualizer.saveTiles(tiles, "NE_roads_douglas", opts)
      //MVTDataVisualizer.saveTiles(tiles, "counties_BASELINE", opts)


      //MVTDataVisualizer.saveTilesGreedyTarlan(tiles, "roads_Sep2_greedy_500", opts)
     // MVTDataVisualizer.saveTilesTarlan(tiles, "provinces_Drop_new_Jul22_col_250", opts)
      //MVTDataVisualizer.saveTilesCompact(tiles, "provinces_DROP_new_TEMPPP.zip", opts)
    } finally {
      sparkSession.stop()
      // Timing end and calculation
      val endTime = System.nanoTime()
      val duration = (endTime - startTime) / 1e9d // duration in seconds
      println(s"Total execution time: $duration seconds")
    }
    }
  //}
}