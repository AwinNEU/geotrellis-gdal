/*
 * Copyright (c) 2014 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.gdal.io.hadoop

import geotrellis.gdal.{RasterBand, RasterDataSet, Gdal}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.spark.io.hadoop.HdfsUtils
import geotrellis.spark.io.hadoop.HdfsUtils._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging

import GdalInputFormat._


/**
  * This class uses GDAL to attempt to read a raster file.
  *
  * @note GDAL only supports reading files from local filesystem. In order for this InputFormat
  *       to work it must copy the entire input file to hadoop.tmp.dir, if it is not already
  *       on a local file system, and invoke GDAL JNI bindings.
  *
  * @note If the .so/.dylib files are not found in the class path, this will crash gloriously.
  *
  * @note All the blocks need to be shuffled to a single machine for each file. If the file
  *       being ingested is much larger than HDFS block size this will be very inefficient.
  */
class GdalInputFormat extends FileInputFormat[GdalRasterInfo, Tile] {
  override def isSplitable(context: JobContext, fileName: Path) = false

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) =
    new GdalRecordReader
}


object GdalInputFormat {
  case class GdalFileInfo(rasterExtent: RasterExtent, crs: CRS, meta: Map[String, String])
  case class GdalRasterInfo(file: GdalFileInfo, bandMeta: Map[String, String])

  def parseMeta(meta: List[String]): Map[String, String] =
    meta
      .map(_.split("="))
      .map(l => l(0) -> l(1))
      .toMap
}

class GdalRecordReader extends RecordReader[GdalRasterInfo, Tile] {
  private var conf: Configuration = _
  private var file: LocalPath = _
  private var rasterDataSet: RasterDataSet = _
  private var fileInfo: GdalFileInfo = _

  private var bandIndex: Int = 0
  private var bandCount: Int = _

  private var band: RasterBand = null
  private var bandMeta: Map[String, String] = _

  def initialize(split: InputSplit, context: TaskAttemptContext) = {
    val path = split.asInstanceOf[FileSplit].getPath

    conf            = context.getConfiguration
    file            = HdfsUtils.localCopy(conf, path)
    rasterDataSet   = Gdal.open(file.path.toUri.getPath)
    bandCount       = rasterDataSet.bandCount

    fileInfo =
      GdalFileInfo(
        rasterExtent = rasterDataSet.rasterExtent,
        crs          = rasterDataSet.crs.getOrElse(LatLng),
        meta         = parseMeta(rasterDataSet.metadata)
      )
  }

  def close() = file match {
    case LocalPath.Temporary(path) => path.getFileSystem(conf).delete(path, true)
    case LocalPath.Original(_) => // leave it well alone
  }

  def getProgress  = bandIndex / bandCount
  def nextKeyValue = {
    val hasNext = bandIndex < bandCount  // bands are indexed from 1 to bandCount
    if (hasNext) {
      bandIndex += 1
      band = rasterDataSet.band(bandIndex)
      bandMeta = band
        .metadata
        .map(_.split("="))
        .map(l => l(0) -> l(1))
        .toMap
    }
    hasNext
  }
  def getCurrentKey   = GdalRasterInfo(fileInfo, bandMeta)
  def getCurrentValue = band.toTile()
}
