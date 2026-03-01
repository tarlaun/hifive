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
package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Computes the zonal statistics problem for a raster file given a set of computed intersections.
 */
public class ZonalStatisticsCore {

  public static Collector[] computeZonalStatisticsQuadSplit(IRasterReader raster, Geometry[] geometries,
                                                        Class<? extends Collector> collectorClass) {
    try {
      Collector[] results = new Collector[geometries.length];
      for (int iGeom = 0; iGeom < geometries.length; iGeom++) {
        results[iGeom] = collectorClass.newInstance();
        computeZonalStatisticsQuadSplit(raster, geometries[iGeom], results[iGeom]);
      }
      return results;
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException("Error creating collector", e);
    } catch (IOException e) {
      throw new RuntimeException("Error reading raster data", e);
    }
  }

  public static Polygon envelopeToPolygon(GeometryFactory factory, Envelope envelope) {
    CoordinateSequence cs = factory.getCoordinateSequenceFactory().create(5, 3, 1);
    cs.setOrdinate(0, 0, envelope.getMinX());
    cs.setOrdinate(0, 1, envelope.getMinY());
    cs.setOrdinate(1, 0, envelope.getMaxX());
    cs.setOrdinate(1, 1, envelope.getMinY());
    cs.setOrdinate(2, 0, envelope.getMaxX());
    cs.setOrdinate(2, 1, envelope.getMaxY());
    cs.setOrdinate(3, 0, envelope.getMinX());
    cs.setOrdinate(3, 1, envelope.getMaxY());
    cs.setOrdinate(4, 0, envelope.getMinX());
    cs.setOrdinate(4, 1, envelope.getMinY());
    return factory.createPolygon(cs);
  }

  public static void computeZonalStatisticsQuadSplit(IRasterReader raster, Geometry geometry, Collector result) throws IOException {
    Stack<Geometry> geoms = new Stack<>();
    geoms.push(geometry);
    EnvelopeND mbr = new EnvelopeND(geometry.getFactory(), 2);
    while (!geoms.isEmpty()) {
      Geometry subgeom = geoms.pop();
      if (subgeom.getNumPoints() < 50) {
        // Geometry is simple enough to process using the naive algorithm
        ZonalStatistics.computeZonalStatisticsNaive(subgeom, raster, result);
      } else {
        // Geometry is complex. Split into four
        mbr.setEmpty();
        mbr.merge(subgeom);
        // First quadrant
        Geometry quad = envelopeToPolygon(subgeom.getFactory(),
            new Envelope(mbr.getMinCoord(0), mbr.getCenter(0), mbr.getMinCoord(1), mbr.getCenter(1)));
        geoms.push(subgeom.intersection(quad));
        // Second quadrant
        quad = envelopeToPolygon(subgeom.getFactory(),
            new Envelope(mbr.getCenter(0), mbr.getMaxCoord(0), mbr.getMinCoord(1), mbr.getCenter(1)));
        geoms.push(subgeom.intersection(quad));
        // Third quadrant
        quad = envelopeToPolygon(subgeom.getFactory(),
            new Envelope(mbr.getMinCoord(0), mbr.getCenter(0), mbr.getCenter(1), mbr.getMaxCoord(1)));
        geoms.push(subgeom.intersection(quad));
        // Fourth quadrant
        quad = envelopeToPolygon(subgeom.getFactory(),
            new Envelope(mbr.getCenter(0), mbr.getMaxCoord(0), mbr.getCenter(1), mbr.getMaxCoord(1)));
        geoms.push(subgeom.intersection(quad));
      }
    }
  }

  public static Collector[] computeZonalStatisticsMasking(IRasterReader raster, Geometry[] geometries,
                                                            Class<? extends Collector> collectorClass) {
    try {
      Collector[] results = new Collector[geometries.length];
      for (int iGeom = 0; iGeom < geometries.length; iGeom++) {
        results[iGeom] = collectorClass.newInstance();
        computeZonalStatisticsMasking(raster, geometries[iGeom], results[iGeom]);
      }
      return results;
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException("Error creating collector", e);
    } catch (IOException e) {
      throw new RuntimeException("Error reading raster data", e);
    }
  }

  public static void computeZonalStatisticsMasking(IRasterReader raster, Geometry geometry, Collector result) throws IOException {
    Point.Double corner1 = new Point.Double();
    Point.Double corner2 = new Point.Double();
    EnvelopeND mbr = new EnvelopeND(geometry.getFactory(), 2);
    mbr.merge(geometry);
    raster.metadata().modelToGrid(mbr.getMinCoord(0), mbr.getMinCoord(1), corner1);
    raster.metadata().modelToGrid(mbr.getMaxCoord(0), mbr.getMaxCoord(1), corner2);
    int i1 = (int) Math.max(0, Math.min(corner1.x, corner2.x));
    int i2 = (int) Math.min(raster.metadata().rasterWidth(), Math.ceil(Math.max(corner1.x, corner2.x)));
    int j1 = (int) Math.max(0, Math.min(corner1.y, corner2.y));
    int j2 = (int) Math.min(raster.metadata().rasterHeight(), Math.ceil(Math.max(corner1.y, corner2.y)));

    List<org.locationtech.jts.geom.Polygon> polygons = new ArrayList<>();
    if (geometry.getGeometryType().equals("Polygon")) {
      polygons.add((org.locationtech.jts.geom.Polygon) (geometry));
    } else if (geometry.getGeometryType().equals("MultiPolygon")) {
      MultiPolygon multipoly = (MultiPolygon) geometry;
      for (int iPolygon = 0; iPolygon < multipoly.getNumGeometries(); iPolygon++) {
        Polygon poly = (Polygon) multipoly.getGeometryN(iPolygon);
        polygons.add(poly);
      }
    }
    BufferedImage mask = new BufferedImage(i2 - i1, j2 - j1, BufferedImage.TYPE_INT_ARGB);
    Graphics graphics = mask.createGraphics();
    graphics.setColor(Color.WHITE);
    graphics.fillRect(0, 0, i2 - i1, j2 - j1);
    for (org.locationtech.jts.geom.Polygon poly : polygons) {
      LinearRing outerRing = (LinearRing) poly.getExteriorRing();
      graphics.setColor(Color.BLACK);
      fillPolygonJTS(graphics, i2 - i1, j2 - j1, mbr, outerRing);
      graphics.setColor(Color.WHITE);
      for (int iRing = 0; iRing < poly.getNumInteriorRing(); iRing++) {
        LinearRing innerRing = (LinearRing) poly.getInteriorRingN(iRing);
        fillPolygonJTS(graphics, i2 - i1, j2 - j1, mbr, innerRing);
      }
    }
    graphics.dispose();
    ITile<float[]> currentTile = null;
    int currentTileID = -1;
    for (int iPixel = i1; iPixel < i2; iPixel++)
      for (int jPixel = j1; jPixel < j2; jPixel++) {
        int color = mask.getRGB(iPixel - i1, jPixel - j1) & 0xff;
        if (color == 0) {
          // Black pixel. Inside the polygon. Process it.
          int tileID = raster.metadata().getTileIDAtPixel(iPixel, jPixel);
          if (tileID != currentTileID) {
            currentTileID = tileID;
            currentTile = raster.readTile(tileID);
          }
          if (!currentTile.isEmpty(iPixel, jPixel)) {
            float[] values = currentTile.getPixelValue(iPixel, jPixel);
            if (result.getNumBands() == 0)
              result.setNumBands(values.length);
            result.collect(iPixel, jPixel, values);
          }
        }
      }
  }

  public static void fillPolygon(Graphics graphics, int imageWidth, int imageHeight, EnvelopeND spaceMBR, LineString ring) {
    int numPoints = ring.getNumPoints();
    int[] xs = new int[numPoints];
    int[] ys = new int[numPoints];
    for (int iPoint = 0; iPoint < numPoints; iPoint++) {
      Coordinate point = ring.getCoordinateN(iPoint);
      xs[iPoint] = (int) ((point.getX() - spaceMBR.getMinCoord(0)) * imageWidth / spaceMBR.getSideLength(0));
      ys[iPoint] = (int) ((point.getY() - spaceMBR.getMinCoord(1)) * imageHeight / spaceMBR.getSideLength(1));
    }
    graphics.fillPolygon(xs, ys, numPoints);
  }


  public static void fillPolygonJTS(Graphics graphics, int imageWidth, int imageHeight, EnvelopeND spaceMBR, LinearRing ring) {
    int numPoints = ring.getNumPoints();
    int[] xs = new int[numPoints];
    int[] ys = new int[numPoints];
    for (int iPoint = 0; iPoint < numPoints; iPoint++) {
      Coordinate point = ring.getCoordinateN(iPoint);
      xs[iPoint] = (int) ((point.getX() - spaceMBR.getMinCoord(0)) * imageWidth / spaceMBR.getSideLength(0));
      ys[iPoint] = (int) ((point.getY() - spaceMBR.getMinCoord(1)) * imageHeight / spaceMBR.getSideLength(1));
    }
    graphics.fillPolygon(xs, ys, numPoints);
  }
}
