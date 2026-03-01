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
package edu.ucr.cs.bdlab.beast.io.tiff;

import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An interface for TIFF readers which is used to abstract the parsing of regular and big TIFF files.
 */
public interface ITiffReader extends Closeable {

  /**
   * Auto detect the input file as either regular or Big TIFF file and returns the appropriate reader.
   * @param in the input stream to the TIFF file
   * @return the header of the file
   * @throws IOException if an error happens while reading the file
   */
  static ITiffReader openFile(FSDataInputStream in) throws IOException {
    short order = in.readShort();
    short signature = order == TiffConstants.LITTLE_ENDIAN? IOUtil.readShortLittleEndian(in) : IOUtil.readShortBigEndian(in);
    ITiffReader reader;
    if (signature == TiffConstants.SIGNATURE)
      reader = new TiffReader();
    else if (signature == TiffConstants.BIG_SIGNATURE)
      reader = new BigTiffReader();
    else
      throw new RuntimeException(String.format("Unrecognized signature %d", signature));
    in.seek(0);
    reader.initialize(in);
    return reader;
  }

  /***
   * Opens a GeoTIFF file given its path.
   * @param fs the file system that contains the GeoTIFF file
   * @param p the path to the file
   * @return an initialized TIFF reader
   * @throws IOException if an error happens while reading the file.
   */
  static ITiffReader openFile(FileSystem fs, Path p) throws IOException {
    ITiffReader reader = openFile(fs.open(p));
    reader.setFilePath(p.toString());
    return reader;
  }

  /**
   * Sets the path of the GeoTIFF file if known.
   * @param path
   */
  void setFilePath(String path);

  /**
   * Returns the path of the underlying GeoTIFF file or {@code null} if not set.
   * @return the path to the GeoTIFF file if known, or {@code null} if unknown or unset.
   */
  String getFilePath();

  /**
   * Initialize the reader by reading the header and IFD entry tables without actually reading any of the raster data.
   * @param in the input stream to the TIFF file
   * @throws IOException if an error happens while reading the file
   */
  void initialize(FSDataInputStream in) throws IOException;

  /**
   * Number of separate images (layers) in this file.
   * @return the number of layers in the input
   */
  int getNumLayers();

  /**
   * Returns a buffer that contains the data of the given entry. If the entry has small data that is stored within
   * the offset attribute, this value is put into the return buffer. If the data is bigger than that, the contents
   * are read from the file into the returned buffer. If possible, the given buffer is reused to read the data but
   * if it is {@code null} or too small to fit the data, a new buffer is created and returned. The position of the
   * returned buffer is always set to zero and the limit is set to the size of the data of the given entry. If the
   * given entry could not be read, the position and limit of the returned buffer are both set to zero to indicate
   * no data.
   * @param entry the entry to read
   * @param buffer the buffer to write the data to if it has enough space
   * @return either the given buffer if the data was written to it, or a new buffer that holds the read data
   * @throws IOException if an error happens while reading the entry
   */
  ByteBuffer readEntry(AbstractIFDEntry entry, ByteBuffer buffer) throws IOException;

  /**
   * Returns the raster data of the given layer number (0-based).
   * @param i the index of the layer to read
   * @return the raster that represents that layer
   * @throws IOException if an error happens while reading that layer
   */
  TiffRaster getLayer(int i) throws IOException;

  /**
   * Returns the list of directory entries for the given layer or {@code null} if the layer does not exist.
   * @param iLayer the index of the layer to read its directory entries
   * @return the array of entries that represent the given layer
   */
  AbstractIFDEntry[] getDirectoryEntries(int iLayer);

  /**
   * Reads a chunk of the file as raw data.
   * @param tileOffset offset from the beginning of the file (as it appears in the offset attribute of IFD entries)
   * @param bytes the array to write the data in. This function attempts to read the entire buffer.
   * @throws IOException if an error happens while reading the file
   */
  void readRawData(long tileOffset, byte[] bytes) throws IOException;

  /**
   * Checks if the file is stored in little endian representation
   * @return {@code true} if this file is stored in LittleEndian
   */
  boolean isLittleEndian();
}
