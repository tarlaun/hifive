package edu.ucr.cs.bdlab.raptor

/**
 * Flatten a list of intersections into tuples in the format
 * (FileTileID: Long, (GeometryID: Long, y: Int, x1: Int, x2: Int)) where
 *  - FileTileID is a 64-bit long where the higher 32-bits represent the file ID and the lower 32-bits represent the tile ID
 *  - GeometryID is a unique ID for each geometry or feature
 *  - y is the scan line index of the intersection in pixel space
 *  - x1 is the index of the first column in the raster (i.e., pixel) in the intersections (inclusive)
 *  - x2 is the index at which that intersection ends (inclusive)
 *
 * @param FileIDs a list of IDs that represent the files for each intersection
 * @param intersectionsList a list of intersections
 */
class CompactIntersectionsIterator(FileIDs:Array[Int], intersectionsList : Array[CompactIntersections])
  extends Iterator[(Long, PixelRange)]  {
  assert(!intersectionsList.isEmpty && intersectionsList(0).getNumIntersections != 0,
      "All intersections cannot be empty")

  var fileTileID: Long = _
  var currentIntersectionIter: java.util.Iterator[CompactIntersections.Intersection] = _
  var currentIntersection: CompactIntersections.Intersection = _
  var currentList: Int = 0

  while (currentList < intersectionsList.length && intersectionsList(currentList) == null)
    currentList += 1

  if (currentList < intersectionsList.length)
    currentIntersectionIter = intersectionsList(currentList).iterator()

  override def hasNext: Boolean = currentList < intersectionsList.length

  /**
   * Prefetches the next record and returns the value of that tuple or null if end of file is reached
   * @return either the next tuple or null
   */
  def next: (Long, PixelRange) = {
    if (!hasNext)
      return null
    currentIntersection = currentIntersectionIter.next()
    fileTileID = (FileIDs(currentList).toLong << 32) | currentIntersection.tileID
    val retVal = (fileTileID, PixelRange(currentIntersection.featureID, currentIntersection.y,
      currentIntersection.x1, currentIntersection.x2))
    // Move to the next intersection
    if (!currentIntersectionIter.hasNext) {
      currentList += 1
      while (currentList < intersectionsList.length && intersectionsList(currentList) == null)
        currentList += 1

      if (currentList < intersectionsList.length)
        currentIntersectionIter = intersectionsList(currentList).iterator()
    }

    retVal
  }

}
