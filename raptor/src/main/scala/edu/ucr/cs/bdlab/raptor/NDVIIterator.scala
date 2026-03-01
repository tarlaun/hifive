package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.ITile

import java.awt.geom.Point2D
import java.io.{File, FileFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class NDVIIterator(iterator: Iterator[(Long, PixelRange)], rasterList: Array[File],rasterList1: Array[String])
  extends Iterator[((Double,Double),Float,String)]{
  lazy val opts: BeastOptions = new BeastOptions()

  var X1: Int = -1
  var X2: Int = -2
  var Y: Int = -1
  var PolygonIndex: Long = -1
  var TileID: Int = -1
  var FileID: Int = -1
  var rasterB4: GeoTiffReader[Float] = _
  var rasterB5: GeoTiffReader[Float] = _
  var date: String = "empty"
  var nextval:(/*Point2D.Double*/(Double,Double),Float, String) = (null,0,null)
  //nextinternal()

  var currentTileID: Int = -1
  var currentTileB4: ITile[Float] = _
  var currentTileB5: ITile[Float] = _

  override def hasNext: Boolean = {

    var temp = nextinternal()

    while(temp._2 == 0 && iterator.hasNext)
      temp = nextinternal()

    if(!iterator.hasNext || temp._2.isNaN)
      false
    else
      true


  }//!(nextval._2.isNaN)

  def nextinternal():(/*Point2D.Double*/ (Double,Double),Float, String)={

    if((X1 == -1) && (X2 == -2) && iterator.hasNext){

      val x = iterator.next()
      X1 = x._2.x1
      X2 = x._2.x2
      Y = x._2.y
      PolygonIndex = x._2.geometryID
      TileID = (x._1 & 0xffffffff).toInt
      val temp = FileID
      FileID = (x._1 >> 32).toInt
      if(temp!=FileID){

        if(rasterB4!=null)
        {
          rasterB4.close()
          rasterB5.close()
        }

        val conf_file = new Configuration()
        val b4 = (rasterList(FileID)).listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = pathname.getName.toLowerCase().endsWith("b4.tif")
        })(0).toString()
        val b5 = (rasterList(FileID)).listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = pathname.getName.toLowerCase().endsWith("b5.tif")
        })(0).toString()
        date = rasterList1(FileID).substring(15,23)
        rasterB4 = new GeoTiffReader()
        rasterB5 = new GeoTiffReader()
        rasterB4.initialize(new Path(b4).getFileSystem(conf_file), b4, "0", opts)
        rasterB5.initialize(new Path(b5).getFileSystem(conf_file), b5, "0", opts)
      }
      val tileID = (x._1 & 0xffffffffL).toInt
      if (currentTileID != tileID) {
        currentTileID = tileID
        currentTileB4 = rasterB4.readTile(tileID)
        currentTileB5 = rasterB5.readTile(tileID)
      }
    }

    /*if(X1>X2 && !iterator.hasNext)
      {
        nextval = (null,Float.NaN,date)
        return nextval
      }*/
    while (X2 >= X1 && (currentTileB4.isEmpty(X1, Y) || currentTileB5.isEmpty(X1, Y))) {
      X1 = X1 +1
    }
    if (X1 <= X2) {
      val pixelValuesB4 = currentTileB4.getPixelValue(X1, Y)
      val pixelValuesB5= currentTileB5.getPixelValue(X1, Y)
      X1 = X1 + 1
      val point = new Point2D.Double()
      currentTileB4.rasterMetadata.gridToModel(X1-1,Y,point)
      nextval = ((point.x, point.y),(pixelValuesB5-pixelValuesB4)/(pixelValuesB4+pixelValuesB5),date)
    }

    if (X1 > X2 && !iterator.hasNext) {
      rasterB4.close()
      rasterB5.close()
      //nextval = (point,Float.NaN,date)

    }

    if (X1 > X2 && iterator.hasNext) {
      X1 = -1
      X2 = -2
    }


    nextval
  }

  override def next(): (/*Point2D.Double*/(Double,Double), Float,String) = {

    var currentval = nextval
    //while(nextval._2==0 && iterator.hasNext)
     //{
       //currentval = nextinternal()

     //}
   // if(iterator.hasNext)
     // nextinternal()
   // System.out.println(currentval)
    currentval

  }

}
