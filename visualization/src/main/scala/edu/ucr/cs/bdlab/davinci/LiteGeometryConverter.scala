package edu.ucr.cs.bdlab.davinci

import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, LinearRing, Point, Polygon, LineString, MultiPoint}
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import edu.ucr.cs.bdlab.davinci._

object LiteGeometryConverter {

  val geometryFactory = new GeometryFactory()

  def convertLiteGeometry(liteGeom: LiteGeometry): Geometry = liteGeom match {
    case point: LitePoint =>
      convertLitePoint(point)
    case multiPoint: LiteMultiPoint =>
      convertLiteMultiPoint(multiPoint)
    case lineString: LiteLineString =>
      convertLiteLineString(lineString)
    case polygon: LitePolygon =>
      convertLitePolygon(polygon)
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported LiteGeometry type: ${liteGeom.getClass.getName}")
  }

  def convertLitePoint(litePoint: LitePoint): Point = {
    val coord = new Coordinate(litePoint.x.toDouble, litePoint.y.toDouble)
    geometryFactory.createPoint(coord)
  }

  def convertLiteMultiPoint(liteMultiPoint: LiteMultiPoint): MultiPoint = {
    val coords = liteMultiPoint.xs.zip(liteMultiPoint.ys).map {
      case (x, y) => new Coordinate(x.toDouble, y.toDouble)
    }
    geometryFactory.createMultiPointFromCoords(coords)
  }

  def convertLiteLineString(liteLineString: LiteLineString): LineString = {
    val coords = liteLineString.parts.flatMap(part => part.xs.zip(part.ys).map {
      case (x, y) => new Coordinate(x.toDouble, y.toDouble)
    })
    geometryFactory.createLineString(coords.toArray)
  }

  def convertLitePolygon(litePolygon: LitePolygon): Polygon = {
    val exteriorRingCoords = litePolygon.exteriorRing.xs.zip(litePolygon.exteriorRing.ys).map {
      case (x, y) => new Coordinate(x.toDouble, y.toDouble)
    }
    val exteriorRing: LinearRing = geometryFactory.createLinearRing(exteriorRingCoords.toArray)

    val interiorRings: Array[LinearRing] = (0 until litePolygon.numInteriorRings).map { i =>
      val interiorRingCoords = litePolygon.interiorRing(i).xs.zip(litePolygon.interiorRing(i).ys).map {
        case (x, y) => new Coordinate(x.toDouble, y.toDouble)
      }
      geometryFactory.createLinearRing(interiorRingCoords.toArray)
    }.toArray

    geometryFactory.createPolygon(exteriorRing, interiorRings)
  }
}
