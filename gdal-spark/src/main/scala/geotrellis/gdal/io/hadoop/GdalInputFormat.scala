/*
 * Copyright 2017 Azavea
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

package geotrellis.gdal.io.hadoop

import geotrellis.gdal._
import geotrellis.gdal.io.hadoop.GdalInputFormat._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.spark.io.hadoop.HdfsUtils
import geotrellis.spark.io.hadoop.HdfsUtils._

import org.gdal.gdal._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

import java.util.regex.Pattern
import scala.collection.JavaConverters._

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
  private val hasDriveLetterSpecifier = Pattern.compile("^/?[a-zA-Z]:")
  private var conf: Configuration = _
  private var file: LocalPath = _
  private var gdalDataset: Dataset = _
  private var reader: GDALReader = _
  private var fileInfo: GdalFileInfo = _

  private var bandIndex: Int = 0
  private var bandCount: Int = _

  private var band: Band = null
  private var bandRaster: Raster[MultibandTile] = null
  private var bandMeta: Map[String, String] = _


  def initialize(split: InputSplit, context: TaskAttemptContext) = {
    val path = split.asInstanceOf[FileSplit].getPath
    val hasDriveLetterSpecifier: Pattern = Pattern.compile("^/?[a-zA-Z]:")

    conf            = context.getConfiguration
    file            = HdfsUtils.localCopy(conf, path)
    val hadoopfilename = file.path.toUri.getPath
    val filename = if (Path.WINDOWS && hasDriveLetterSpecifier.matcher(hadoopfilename).find())
      hadoopfilename.tail
    else
      hadoopfilename
    gdalDataset     = GDAL.open(filename)
    reader          = GDALReader(gdalDataset)
    bandCount       = gdalDataset.getRasterCount

    fileInfo =
      GdalFileInfo(
        rasterExtent = gdalDataset.rasterExtent,
        crs          = gdalDataset.crs.getOrElse(LatLng),
        meta         = parseMeta(gdalDataset.GetMetadata_List("").asScala.toList.map(_.asInstanceOf[String]))
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
      band = gdalDataset.GetRasterBand(bandIndex)
      bandRaster = reader.readRaster(bands = Seq(bandIndex - 1))
      bandMeta = band
        .GetMetadata_List("").asScala.toList.map(_.asInstanceOf[String])
        .map(_.split("="))
        .map(l => l(0) -> l(1))
        .toMap
    }
    hasNext
  }
  def getCurrentKey   = GdalRasterInfo(fileInfo, bandMeta)
  def getCurrentValue = bandRaster.tile.band(0)
}
